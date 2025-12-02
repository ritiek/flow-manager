def task1():
    return {"records": 100, "source": "database"}


def task2(records, source):
    return {"processed_records": records - 5, "invalid_records": 5}


def fail1(processed_records, invalid_records):
    inserted_records = processed_records // 0
    # inserted_records = processed_records // 5
    print(inserted_records)
    return {"inserted_records": inserted_records}


def task3(inserted_records=0):
    return {"completed_records": inserted_records}
