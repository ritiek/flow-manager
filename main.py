from flask import Flask, jsonify, request
import json
import tasks
import inspect

task_registry = {}


class FlowEngine:
    def __init__(self, registry):
        self.registry = registry

    def execute(self, flow):
        log = []
        current = flow["start_task"]
        previous_task_outputs = {}
        flow_failed = False

        while current != "end":
            if current not in self.registry:
                return {
                    "status": "failure",
                    "message": f"Task '{current}' not found in registry",
                    "execution_log": log,
                }

            condition = next(
                (c for c in flow["conditions"] if c["source_task"] == current), None
            )
            expected_outcome = condition["outcome"] if condition else None

            try:
                task_func = self.registry[current]
                sig = inspect.signature(task_func)
                param_names = list(sig.parameters.keys())

                task_kwargs = {
                    k: v for k, v in previous_task_outputs.items() if k in param_names
                }

                result = task_func(**task_kwargs)
                current_task_outputs = result
                log.append(
                    {
                        "task": current,
                        "expected_outcome": expected_outcome,
                        "success": True,
                        "output": result,
                    }
                )
                task_succeeded = True
            except Exception as e:
                log.append(
                    {
                        "task": current,
                        "expected_outcome": expected_outcome,
                        "success": False,
                        "output": None,
                    }
                )
                task_succeeded = False
                current_task_outputs = {}

            next_task = self._get_next_task(flow, current, task_succeeded)
            if next_task is None:
                break

            if next_task != "end" and next_task in self.registry:
                next_task_func = self.registry[next_task]
                next_sig = inspect.signature(next_task_func)
                next_param_names = set(next_sig.parameters.keys())

                incompatible_keys = [
                    key
                    for key in current_task_outputs.keys()
                    if key not in next_param_names
                ]

                if incompatible_keys:
                    return {
                        "status": "failure",
                        "message": f"Task '{next_task}' does not accept outputs from '{current}': {incompatible_keys}",
                        "execution_log": log,
                    }

                next_required_params = [
                    param
                    for param in next_sig.parameters.values()
                    if param.default == inspect.Parameter.empty
                ]

                next_missing_params = [
                    param.name
                    for param in next_required_params
                    if param.name not in current_task_outputs
                ]

                if next_missing_params:
                    return {
                        "status": "failure",
                        "message": f"Task '{next_task}' is missing required parameters from previous task '{current}': {next_missing_params}",
                        "execution_log": log,
                    }

            condition_matched = self._condition_was_met(flow, current, task_succeeded)
            if not condition_matched:
                flow_failed = True

            previous_task_outputs = current_task_outputs
            current = next_task

        final_status = "failed" if flow_failed else "completed"

        return {
            "status": final_status,
            "flow_id": flow["id"],
            "flow_name": flow["name"],
            "execution_log": log,
        }

    def _get_next_task(self, flow, current_task, task_succeeded):
        condition = next(
            (c for c in flow["conditions"] if c["source_task"] == current_task), None
        )

        if not condition:
            return None

        desired_outcome = condition["outcome"]
        actual_outcome = "success" if task_succeeded else "failure"

        if actual_outcome == desired_outcome:
            return condition.get("target_task_success")
        else:
            return condition.get("target_task_failure")

    def _condition_was_met(self, flow, current_task, task_succeeded):
        condition = next(
            (c for c in flow["conditions"] if c["source_task"] == current_task), None
        )

        if not condition:
            return True

        desired_outcome = condition["outcome"]
        actual_outcome = "success" if task_succeeded else "failure"

        return actual_outcome == desired_outcome


def load_tasks():
    with open("tasks.json") as f:
        config = json.load(f)

    for task_def in config["flow"]["tasks"]:
        task_name = task_def["name"]
        task_func = getattr(tasks, task_name)
        task_registry[task_name] = task_func


app = Flask(__name__)
engine = FlowEngine(task_registry)


@app.post("/execute")
def execute_flow():
    flow = request.json["flow"]
    result = engine.execute(flow)
    return jsonify(result)


@app.get("/tasks")
def list_tasks():
    return {"tasks": list(task_registry.keys())}


if __name__ == "__main__":
    load_tasks()
    app.run(port=5000, debug=True)
