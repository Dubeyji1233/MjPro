import pandas as pd

# Specify the path to your Excel file

# Read the data from Excel into a pandas DataFrame
df = pd.read_csv(r"C:\Users\AbhishekDubey\PycharmProjects\MJ-Pro\usernames.csv")

# Create a new DataFrame for the desired output
output_df = pd.DataFrame(columns=['City', 'Date', 'Amount', 'Percentage'])

print(df)

# Iterate through each row and extract information
for index, row in df.iterrows():
    city = str(row['City'])
    date = str(row['Date'])
    amount = str(row['Amount'])
    percentage = str(row['Percentage'])

    # Check the format of the columns and adjust accordingly
    if '%' in amount:
        amount, percentage = percentage, amount

    # Append the data to the output DataFrame
    output_df = pd.concat([output_df, pd.DataFrame({'City': [city], 'Date': [date], 'Amount': [amount], 'Percentage': [percentage]})], ignore_index=True)

# Print the final DataFrame
print(output_df)
