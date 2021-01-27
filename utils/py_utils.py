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

def write_csv(filepath, data):
    with open(filepath, 'w', newline='') as csv_file:
        headers = data[0].keys()
        csv_writer = csv.DictWriter(csv_file, delimiter=',', fieldnames=headers)
        line_count = 0
        for row in data:
            if line_count == 0:
                csv_writer.writeheader()
            csv_writer.writerow(row)
            line_count += 1
        print(f'wrote {line_count} rows to {filepath}')

def dict_pick(d, keys):
    #return {x: d[x] for x in d.keys() if x in keys}
    new_dict = {}
    for k in keys:
        if k in d:
            new_dict[k] = d[k]
        else:
            new_dict[k] = None
    return new_dict

def dict_omit(d, keys):
    return {x: d[x] for x in d if x not in keys}

def dedupe(_list):
    print(f'dedupe recieved: {_list}')
    deduped_list = []
    for val in _list:
        if val not in deduped_list:
            deduped_list.append(val)
    print(f'dedupe returning: {deduped_list}')
    return deduped_list