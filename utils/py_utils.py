import csv

def read_csv(filepath):
    with open(filepath) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        headers = []
        data = []
        for row in csv_reader:
            if line_count == 0:
                headers = row
                line_count += 1
            else:
                data.append(dict(zip(headers, row)))
                line_count += 1
        print(f'read {line_count} rows from {filepath}')
        return data

def dict_pick(d, keys):
    return {x: d[x] for x in d.keys() if x in keys}

def dict_omit(d, keys):
    return {x: d[x] for x in d if x not in keys}