import os
import odml
import pandas as pd
from datetime import datetime as dt
import json

def create_bids_directory_structure(subj_sess_runs, base_dir, ephys_files, task):
    """
    Creates a BIDS directory structure for each subject, session, and run.
    
    Parameters:
    subj_sess_runs (dict): A dictionary mapping subject identifiers to session and run information.
    base_dir (str): The base directory where the BIDS structure will be created.
    ephys_files (list): List of electrophysiology file names to be created in each session directory.
    task (str): The task identifier to be included in the file names.
    """
    for subj, sess_runs in subj_sess_runs.items():
        for sess_run in sess_runs:
            for sess, run in sess_run.items():
                bids_dir = os.path.join(base_dir, f'sub-{subj}', f'ses-{sess}', 'ephys')
                os.makedirs(bids_dir, exist_ok=True)
                [open(os.path.join(bids_dir, f'sub-{subj}_ses-{sess}_task-{task}_run-{run}_{file}'), 'a').close() for file in ephys_files]

def load_odml_metadata(file_path):
    """
    Loads metadata from an odML file.

    Parameters:
    file_path (str): The path to the odML file.

    Returns:
    odml.Document or None: The loaded odML document or None if loading fails.
    """
    try:
        return odml.load(file_path)
    except Exception as e:
        print(f"Error loading odml file {file_path}: {e}")
        return None
    
def extract_odml_value(odml_section, key):
    """
    Extracts a value from an odML section based on a given key.

    Parameters:
    odml_section (odml.Section): The odML section from which to extract the value.
    key (str): The key of the property to extract, which can include nested sections separated by '/'.

    Returns:
    The value of the specified property or None if the key is not found.
    """
    if '/' in key:
        nested_keys = key.split('/')
        nested_section = odml_section
    
        for nested_key in nested_keys[:-1]:
            nested_section = next((section for section in nested_section.sections if section.name == nested_key), None)
            if nested_section is None:
                return None
        return next((prop.values[0] for prop in nested_section.properties if prop.name == nested_keys[-1]), None)
    else:
        return next((prop.values[0] for prop in odml_section
    .properties if prop.name == key), None)
    
def save_to_tsv(dataframe, path, filename):
    """
    Saves a DataFrame to a TSV file.

    Parameters:
    dataframe (pandas.DataFrame): The DataFrame to be saved.
    path (str): The directory path where the TSV file will be saved.
    filename (str): The name of the TSV file.

    Raises:
    Exception: If an error occurs during saving.
    """
    try:
        dataframe.to_csv(os.path.join(path, filename), sep='\t', index=False)
    except Exception as e:
        print(f"Error saving file {filename}: {e}")
        
def format_datetime_columns(df, date_cols, time_cols):
    """
    Formats the columns in a DataFrame as date or time.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the columns to be formatted.
    date_cols (list): List of column names in the DataFrame to be formatted as dates.
    time_cols (list): List of column names in the DataFrame to be formatted as times.
    """
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error converting column {col} to date format: {e}")

    for col in time_cols:
        try:
            df[col] = df[col].apply(lambda x: x.strftime('%H:%M:%S') if not pd.isnull(x) else None)
        except Exception as e:
            print(f"Error converting column {col} to time format: {e}")
            

def create_json_for_tsv(metadata, tsv_headers, odml_keys, json_file_path, section_name):
    """
    Creates a JSON file describing each column in a TSV file based on odML metadata.

    Parameters:
    metadata (odml.Document): The odML document containing metadata.
    tsv_headers (list): List of headers in the TSV file.
    odml_keys (list): List of odML keys corresponding to the TSV headers.
    json_file_path (str): Path to save the JSON file.
    section_name (str): The name of the section in the odML document to look for metadata.
    """
    description_dict = {}
    section = metadata[section_name]
    for header, key in zip(tsv_headers, odml_keys):
        prop = next((p for p in section.properties if p.name == key), None)
        description_dict[header] = {'Description': prop.definition if prop and prop.definition else 'No definition found'}
    
    with open(json_file_path, 'w') as f:
        json.dump(description_dict, f, indent=4)



def create_participants_df(metadata, participants_tsv_headers, odml_keys):
    """
    Creates a DataFrame for participants based on metadata.

    Parameters:
    metadata (odml.Document): The odML document containing participant metadata.
    participants_tsv_headers (list): List of headers for the participants TSV file.
    odml_keys (list): List of odML keys corresponding to the TSV headers.

    Returns:
    dict: A dictionary representing a row in the participants DataFrame.
    """
    return {header: metadata['Subject'].properties[key].values[0] for header, key in zip(participants_tsv_headers, odml_keys)}


def create_sessions_df(metadata, sessions_tsv_headers, sessions_odml_keys):
    """
    Creates a DataFrame for session information based on metadata.

    Parameters:
    metadata (odml.Document): The odML document containing session metadata.
    sessions_tsv_headers (list): List of headers for the sessions TSV file.
    sessions_odml_keys (list): List of odML keys corresponding to the TSV headers.

    Returns:
    pandas.DataFrame: A DataFrame containing session information.
    """
    sessions_df = pd.DataFrame(columns=sessions_tsv_headers)
    session_odml = metadata['Recording']
    for header, key in zip(sessions_tsv_headers, sessions_odml_keys):
        session_value = extract_odml_value(session_odml, key)
        sessions_df[header] = [session_value]

    format_datetime_columns(sessions_df, date_cols=['session_date'], time_cols=['acq_time'])

    return sessions_df


# Main Execution
subj_sess_runs = {'l': [{'101210': '001'}], 'i': [{'140703': '001'}]}
ephys_files = ['ephys.nix', 'ephys.json', 'channels.tsv', 'contacts.tsv', 'probes.tsv', 'events.tsv']
task = 'r2g'
base_dir = 'r2g_bids'

create_bids_directory_structure(subj_sess_runs, base_dir, ephys_files, task)

monkeyL_metadata = load_odml_metadata('multielectrode_grasp/datasets_blackrock/l101210-001.odml')
monkeyN_metadata = load_odml_metadata('multielectrode_grasp/datasets_blackrock/i140703-001.odml')

participants_tsv_headers = ['participant_id', 'species', 'sex', 'birthdate', 'handedness', 'trivial_name', 'given_name', 'disabilities', 'character']
odml_keys = ['Identifier', 'Species', 'Gender', 'Birthday', 'ActiveHand', 'TrivialName', 'GivenName', 'Disabilities', 'Character']

monkeyL_subject_dict = create_participants_df(monkeyL_metadata, participants_tsv_headers, odml_keys)
monkeyN_subject_dict = create_participants_df(monkeyN_metadata, participants_tsv_headers, odml_keys)

participants_df = pd.DataFrame([monkeyL_subject_dict, monkeyN_subject_dict])
save_to_tsv(participants_df, base_dir, 'participants.tsv')

participants_json_path = os.path.join(base_dir, 'participants.json')
create_json_for_tsv(monkeyL_metadata, participants_tsv_headers, odml_keys, participants_json_path, 'Subject')

sessions_tsv_headers = ['session_id', 'acq_time', 'session_date', 'session_weekday', 'is_noisy', 'session_duration', 'is_spikesorted',  'task', 'comment', 'number_of_trials', 'number_of_correct_trials', 'number_of_grip_error_trials', 'standard_settings']
sessions_odml_keys = ['RecordingDay', 'Time', 'Date', 'Weekday', 'Noisy', 'Duration', 'IsSpikeSorted', 'TaskType', 'Comment', 'TaskSettings/TotalTrialCount', 'TaskSettings/CorrectTrialCount', 'TaskSettings/GripErrorTrialCount', 'TaskSettings/StandardSettings']

monkeyL_sessions_df = create_sessions_df(monkeyL_metadata, sessions_tsv_headers, sessions_odml_keys)
monkeyN_sessions_df = create_sessions_df(monkeyN_metadata, sessions_tsv_headers, sessions_odml_keys)

sessions_df = pd.concat([monkeyL_sessions_df, monkeyN_sessions_df], ignore_index=True)
save_to_tsv(sessions_df, base_dir, 'sessions.tsv')

sessions_json_path = os.path.join(base_dir, 'sessions.json')
create_json_for_tsv(monkeyL_metadata, sessions_tsv_headers, sessions_odml_keys, sessions_json_path, 'Recording')

## The x, y and z coordinates culd not be retrieved directly - nned more explanation - same for alpha, beta and gamma rotation angles
# probes_tsv_headers = ['probe_id', 'type', 'manufacturer', 'device_serial_number', 'contact_count', 'width', 'height', 'depth', 'dimension_unit', 'coordinate_reference_point', 'hemisphere', 'associated_brain_region', 'associated_brain_region_quality_type', 'reference_atlas', 'material']
probes_tsv_headers = ['manufacturer', 'device_serial_number', 'contact_count', 'width', 'height', 'material', 'probe_geometry','active_contacts', 'used_contacts' ]
probes_odml_keys = ['Manufacturer', 'SerialNo', 'Array/ElectrodeCount', 'Array/Grid_01/GridWidth', 'Array/Grid_01/GridLength', 'Array/Grid_01/ElectrodeMetal', 'Array/Grid_01/ElectrodeGeometry', 'Array/ActiveElectrodeCount', 'Array/Grid_01/UsedElectrodeCount']

# Notes
# There should be a mention of available and active and used electrodes - maybe??
# Not sure about - height and depth and how they relate?
# Handling of the dimensions unit of the prbe is wierd - needs to be added seperately - too much work to automate
# ELectrode etal, the material doesn't then necessarily belong in the probe file???
# Need probe geometery??

