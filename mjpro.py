import pandas as pd

df = pd.read_csv(r"C:\Users\AbhishekDubey\PycharmProjects\MJ-Pro\usernames.csv")
# Initialize lists for each column
cities, dates, amounts, percentages = [], [], [], []

# Iterate through rows
for index, row in df.iterrows():
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
            print(city)
            cities.append(city)

# Create a new DataFrame
new_data = {'City': cities, 'Date': dates, 'Amount': amounts, 'Percentage': percentages}
new_df = pd.DataFrame(new_data)

# Print the result
print(new_df)
