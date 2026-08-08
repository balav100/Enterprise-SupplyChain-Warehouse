import subprocess


checks = [
    "check_row_counts.py",
    "check_nulls.py",
    "check_duplicates.py",
    "check_business_rules.py"
]


print("\nSTARTING DATA QUALITY PIPELINE")
print("=" * 50)


for check in checks:

    print(f"\nRunning {check}")

    result = subprocess.run(
        ["python", f"data_quality/{check}"]
    )

    if result.returncode != 0:
        print(f"{check} FAILED")
        break

    else:
        print(f"{check} PASSED")


print("\nDATA QUALITY PIPELINE COMPLETED")