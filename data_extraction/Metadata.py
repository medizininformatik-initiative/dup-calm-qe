import json
import os
from datetime import datetime

os.makedirs('fhir_results', exist_ok=True)


def gather_metadata(source, count):

    if os.path.exists('fhir_results/metadata.json'):
        with open('fhir_results/metadata.json', 'r') as metadata_file:
            metadata = json.load(metadata_file)
    else:
        metadata = {
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "asthma_and_copd_patient_count": 0,
            "asthma_and_copd_patients_with_chief_complaint": 0,
            "patient_count_with_secondary_conditions": 0,
            "patient_count_with_observations": 0,
            "patient_count_with_medications": 0
        }

    metadata["execution_date"] = datetime.now().strftime("%Y-%m-%d")
    metadata["execution_time"] = datetime.now().strftime("%H:%M:%S")

    if source in metadata:
        metadata[source] = count
    else:
        print("Unknown source")

    with open('fhir_results/metadata.json', 'w') as metadata_file:
        json.dump(metadata, metadata_file, indent=4)

    print("Metadata has been saved")
