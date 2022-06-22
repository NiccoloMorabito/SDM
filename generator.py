import csv

from loader import generate_mocked_transaction


def generate_csv_with_fake_transactions(file='fake.csv', n=1000000):
    with open(file, 'w+', encoding='utf8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['tid', 'origin', 'destination', 'description', 'price', 'quantity', 'transaction_date'])
        for i in range(n):
            writer.writerow(generate_mocked_transaction())


def aggregate_transactions(file_in='fake.csv', file_out='fake_agg.csv'):
    with open(file_in, 'r', encoding='utf8') as f:
        reader = csv.reader(f)
        next(reader)
        transactions = {}
        for row in reader:
            if row[5] == '0':
                continue
            origin = row[1]
            destination = row[2]
            if f'{origin}->{destination}' not in transactions:
                transactions[f'{origin}->{destination}'] = []
            transactions[f'{origin}->{destination}'].append((round((int(row[4])) / int(row[5]), 2), int(row[5])))
    with open(file_out, 'w+', encoding='utf8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['origin', 'destination', 'price', 'quantity'])
        for country_pair, trans in transactions.items():
            c = country_pair.split('->')
            origin = c[0]
            destination = c[1]
            trans.sort(reverse=True)
            agg = 0
            for price, quantity in trans:
                agg += quantity
                writer.writerow([origin, destination, price, quantity])


if __name__ == '__main__':
    aggregate_transactions()
