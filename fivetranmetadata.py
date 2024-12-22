import requests
import pandas as pd
import configparser as cp

# Logic: Groups> Connectors> Schemas> Tables
# Add config to load secret from config

# Fivetran API key and secret


API_KEY = 'xxx'
API_SECRET = 'xxx'

# Fivetran API endpoint for groups
all_groups_url = 'https://api.fivetran.com/v1/groups'

# Fivetran API from groups to Connectors 
group_connector_url = 'https://api.fivetran.com/v1/groups/{}/connectors'

# Fivetran API endpoint for connector to schema
connector_schema_url = 'https://api.fivetran.com/v1/connectors/{}/schemas'

# Make the API requests for all groups
all_groups_payload = requests.get(all_groups_url, auth=(API_KEY, API_SECRET))

# Check if the request was successful
if all_groups_payload.status_code == 200:
    all_groups_data = all_groups_payload.json()
    all_groups = all_groups_data['data']['items']

    # Prepare a list to store everything
    combined_list = []

    # Iterate through each group
    for group in all_groups:
        group_id = group['id']
        group_name = group['name']
        print(f"Processing Group ID: {group_id} (Name: {group_name})")

        # Create the URL before the request is made:
        current_group_connector_url = group_connector_url.format(group_id)
        print(f"Requesting Group To Connector URL: {current_group_connector_url}")

        # Fetch Connectors for the current group
        group_connector_payload = requests.get(current_group_connector_url, auth=(API_KEY, API_SECRET))
        
        if group_connector_payload.status_code == 200:
            group_connector_data = group_connector_payload.json()
            connectors = group_connector_data.get('data', {}).get('items', [])

            # Iterate through each connector
            for connector in connectors:
                connector_id = connector['id']
                connector_name = connector['schema']
                paused_status = connector['paused']
                sync_freq = connector['sync_frequency']
                print(f"Processing Connector ID: {connector_id} (Name: {connector_name})")

                # Create the URL before the request is made:
                current_schema_table_url = connector_schema_url.format(connector_id)
                print(f"Requesting Connector To Schema URL: {current_schema_table_url}")

                # Fetch schemas for the current connector
                connector_schema_payload = requests.get(current_schema_table_url, auth=(API_KEY, API_SECRET))
                
                if connector_schema_payload.status_code == 200:
                    connector_schema_data = connector_schema_payload.json()
                    schemas = connector_schema_data.get('data', {}).get('schemas', {})

                    # For schemas
                    for schema_name, schema_details in schemas.items():
                        tables = schema_details.get('tables', {})
                        for table_name in tables.keys():
                            combined_list.append({
                                'Destination Name': group_name,
                                'Connector Name': connector_name,
                                "Paused": paused_status,
                                "Sync Frequency": sync_freq,
                                'Schema': schema_name,
                                'Table': table_name
                            })

    # Create a DataFrame from the combined list
    combined_df = pd.DataFrame(combined_list)

    # Export to a single Excel file
    combined_df.to_excel('fivetran_combined_groups_schemas_tables.xlsx', index=False, sheet_name='GroupsSchemasTables')
    print("Data exported to fivetran_combined_groups_schemas_tables.xlsx successfully.")
else:
    print("Failed to retrieve groups data.")
