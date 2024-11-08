import os
from collections import defaultdict
import json
import time

from fhirclient.models.medicationadministration import MedicationAdministration

from Constants import USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, LOINC_SYSTEM_NAME, MAX_WORKERS, ATC_SYSTEM_NAME
from concurrent.futures import ThreadPoolExecutor, as_completed

from FhirHelpersUtils import connect_to_server, fetch_bundle_for_code
from Metadata import gather_metadata

def read_input_code_file(filename):
    """
    :param filename:  input file of code list
    :return:
    """
    with open(filename, "r") as fp:
        lines = json.load(fp)

        if 'loinc_codes' in filename:
            system = LOINC_SYSTEM_NAME
            if not os.path.exists(f"fhir_results/LOINC/"):
                os.makedirs(f"fhir_results/LOINC/")
            code_list = [item['code'] for item in lines['codes']]

        elif 'icd_codes' in filename:
            system = ICD_SYSTEM_NAME
            if not os.path.exists(f"fhir_results/ICD/"):
                os.makedirs(f"fhir_results/ICD/")
            code_list = [code for item in lines['codes'] for code in item['code']]

        elif 'atc_codes' in filename:
            system = ATC_SYSTEM_NAME
            if not os.path.exists(f"fhir_results/ATC/"):
                os.makedirs(f"fhir_results/ATC/")
            code_list = [code['code'] for code in lines]

    return code_list, system

def write_results(entries, patient_counter, code_type):
    """
    It reads all Resources in the bundle and write to output files per patient.
    """
    if code_type == "LOINC":
        whole_path = "fhir_results/LOINC/" + patient_counter + "_patient_observations.json"
    elif code_type == "ICD":
        whole_path = "fhir_results/ICD/" + patient_counter + "_patient_conditions.json"
    elif code_type == "ATC":
        whole_path = "fhir_results/ATC/" + patient_counter + "_patient_medications.json"

    with open(whole_path, 'w') as file:
        json.dump(entries, file, indent=4)



def observations(patient, code_file, source, smart):
    print(f"Creating queries for patient {patient} for observation resources...\n")
    while True:
        try:
            bundle = source.where(struct={'_count': b'1000', 'subject': patient}).perform(smart.server)
            break
        except Exception as exc:
            print(f"Generated an exception: {exc} but continue to trying... \n")
            time.sleep(3)
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

    observations_bundles = fetch_bundle_for_code(smart, bundle)
    code_list, system = read_input_code_file(code_file)
    filtered_results = []
    for observation in observations_bundles:
        if 'code' in observation['resource'] and 'coding' in observation['resource']['code']:
            for coding in observation['resource']['code']['coding']:
                if LOINC_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                    filtered_results.append(observation)
    print(f"Patient {patient} has {len(filtered_results)} observations.")
    return filtered_results

def conditions(patient, code_file, source, smart,):
    code_list, system = read_input_code_file(code_file)
    sub_code_lists = [code_list[i:i + 30] for i in range(0, len(code_list), 30)]  # Smaller chunks of code list
    conditions = []
    print(f"Creating queries for patient {patient} for conditions...\n")
    for sub_code_list in sub_code_lists:
        sub_code_list_str = ','.join([system + '|' + code for code in sub_code_list])
        while True:
            try:
                bundle = source.where(struct={'_count': b'1000', 'subject': patient, 'code': sub_code_list_str}).perform(smart.server)
                break
            except Exception as exc:
                print(f"Generated an exception: {exc} but continue to trying... \n")
                time.sleep(3)
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

        batch_result = fetch_bundle_for_code(smart, bundle)

        if len(batch_result) > 0:
            conditions.extend(batch_result)

    return conditions

def medications(patient, code_file, source, smart):
    code_list, system = read_input_code_file(code_file)
    code_list_str = ','.join([system + '|' + code for code in code_list])

    print(f"Creating queries for patient {patient}...\n")
    while True:
        try:
            if source == MedicationAdministration:
                bundle = source.where(struct={'_count': b'100', 'patient': patient, 'medication.code': code_list_str}).perform(smart.server)
            else:
                bundle = (source.where(struct={'_count': b'1000', 'subject': patient, 'code': code_list_str})
                          .perform(smart.server))
            break
        except Exception as exc:
            print(f"Generated an exception: {exc} but continue to trying... \n")
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
            time.sleep(3)
    medications_bundles = fetch_bundle_for_code(smart, bundle)
    return medications_bundles

def execute_thread_for_fetching(code_file, source, patient_list, code_type, function_to_run):
    """
    Threads for running fetch queries parallel.
    """
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(function_to_run, patient, code_file, source, smart): patient for patient in patient_list}
        counter = 0
        for future in as_completed(future_to_code):
            patient = future_to_code[future]
            try:
                entries = future.result()
                if entries:
                    counter += 1
                    write_results(entries, str(counter), code_type)
                print(f"Processed patient {patient} with {len(entries)} entries.\n")
            except Exception as exc:
                print(f"Patient {patient} generated an exception: {exc}.\n")


    ###META DATA COLLECTION###
    '''
    patient_count_with_secondary_conditions: Number of cohort patients that has secondary conditions (non main diagnosis ASTHMA OR COPD) (TO DO: ADD)
    patient_count_with_observations: Number of cohort patients that has at least one observation
    patient_count_with_medications: Number of cohort patients that has at least one medication
    conditions_counts: Frequency of each ICD code (TO DO: ADD)
    observations_counts:Frequency of each LOINC code (TO DO: ADD)
    medication_counts: Frequency of each ATC code (TO DO: ADD)
    '''

    if code_type == "LOINC":
        gather_metadata("patient_count_with_observations", counter)
    elif code_type == "ICD":    ###TO DO: Complete this part.###
        pass
    elif code_type == "ATC":
        gather_metadata("patient_count_with_medications", counter)
    print("---------------End of Code------------------------")
