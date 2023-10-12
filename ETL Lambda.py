import pandas as pd
import psycopg2
import json

def transform_data(data):
    # Initialize lists for each column
    cities, dates, amounts, percentages = [], [], [], []

    # Iterate through rows
    for row in data:
        city, date, amount, percentage = '', '', '', ''

        # Iterate through items in the row
        for item in row:
            if item.isdigit():
                amount = int(item)
                amounts.append(amount)
            elif '%' in item:
                percentage = item
                percentages.append(percentage)
            elif '/' in item or '-' in item:
                date = item
                dates.append(date)
            else:
                city = item
                cities.append(city)

    # Create a new DataFrame
    new_data = {'City': cities, 'Date': dates, 'Amount': amounts, 'Percentage': percentages}
    new_df = pd.DataFrame(new_data)
    return new_df

def lambda_handler(event, context):
    # Assuming the event contains information needed for your ETL process
    # You may trigger Lambda with events, for example, from an S3 upload or CloudWatch event

    # Connection details for Aurora PostgreSQL
    pg_host = 'your-aurora-hostname'
    pg_port = 5432
    pg_db = 'your-aurora-database'
    pg_user = 'your-aurora-username'
    pg_password = 'your-aurora-password'

    # Connection details for Redshift
    rs_host = 'your-redshift-hostname'
    rs_port = 5439
    rs_db = 'your-redshift-database'
    rs_user = 'your-redshift-username'
    rs_password = 'your-redshift-password'

    try:
        # Connect to Aurora PostgreSQL
        pg_conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_db,
            user=pg_user,
            password=pg_password
        )
        pg_cursor = pg_conn.cursor()

        # Extract data from Aurora PostgreSQL (Modify query as needed)
        pg_cursor.execute('SELECT * FROM your_source_table')
        data = pg_cursor.fetchall()

        # Transform data
        transformed_df = transform_data(data)

        # Connect to Redshift
        rs_conn = psycopg2.connect(
            host=rs_host,
            port=rs_port,
            database=rs_db,
            user=rs_user,
            password=rs_password
        )
        rs_cursor = rs_conn.cursor()

        # Load data into Redshift (Modify as needed, assuming a simple insert)
        for index, row in transformed_df.iterrows():
            rs_cursor.execute('INSERT INTO your_destination_table VALUES (%s, %s, %s, %s)', 
                              (row['City'], row['Date'], row['Amount'], row['Percentage']))

        # Commit changes and close connections
        pg_conn.commit()
        rs_conn.commit()
        pg_conn.close()
        rs_conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps('ETL process completed successfully!')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
