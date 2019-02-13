import json
import dateutil.parser
import operator
import argparse
import csv


def process_getLogs(input_json_file, agentid='Default', print_output=False):
    with open(input_json_file, encoding='utf-8-sig') as json_file:
        json_data = json.load(json_file)

    snapshot_data = []

    # Load the relevant data
    # ToDo: parse the rest of this

    for l, entry in enumerate(json_data):
        if 'objects' in entry:
            if entry['text'] == "GET_AGENT_SNAPSHOT succeeded.":
                snapshot = entry['objects'][0]['snapshot']
                # snapshot_data.append(snapshot)

                data = {}

                if 'state' in snapshot:
                    data = {
                        'agentid': agentid,
                        'linenum': l,
                        'state_time': dateutil.parser.parse(snapshot['state']['startTimestamp']),
                        'state_time_str': snapshot['state']['startTimestamp'],
                        'state_type': snapshot['state']['type'],
                        'agent_state': snapshot['state']['name'],
                    }

                for contact in snapshot['contacts']:

                    if 'state' in contact:
                        data['agent_in_call_state'] = contact['state']['type']
                        data['agent_call_state_time'] = dateutil.parser.parse(contact['state']['timestamp'])
                        data['agent_call_state_time_str'] = contact['state']['timestamp']

                    if 'queue' in contact:
                        data['queue'] = contact['queue']['name']

                    data['connections'] = -1

                    for i, connection in enumerate(contact['connections']):
                        key = "connection%i" % i
                        data['connections'] += 1
                        data['%s_direction' % key] = connection['type']
                        data['%s_state' % key] = connection['state']['type']
                        data['%s_timestamp_str' % key] = connection['state']['timestamp']
                        data['%s_timestamp' % key] = dateutil.parser.parse(connection['state']['timestamp'])
                        if 'endpoint' in connection:
                            if connection['endpoint']['phoneNumber']:
                                data['%s_phone' % key] = connection['endpoint']['phoneNumber']

                snapshot_data.append(data)

    print("lines processed %i" % len(snapshot_data))

    snapshot_data = sorted(snapshot_data, key=operator.itemgetter('state_time'))

    if print_output:
        print("AGENT SNAPSHOT ENTRIES")
        for i in snapshot_data:
            print(i)

    ### Get agent_states
    state_change_data = []

    for i, entry in enumerate(snapshot_data):
        if i + 1 >= len(snapshot_data):
            break
        if snapshot_data[i]['agent_state'] != snapshot_data[i + 1]['agent_state']:
            td = ((snapshot_data[i + 1]['state_time'] - snapshot_data[i]['state_time']).total_seconds())



            data = {
                'agentid': entry['agentid'],
                'type': 'agent',
                'state': entry['agent_state'],
                'state_time': snapshot_data[i]['state_time_str'],
                'duration': td,
            }

            if 'connections' in entry:
                num_connections = entry['connections']
                data['connections'] = num_connections
                if num_connections == 1:
                    data['direction'] = entry['connection1_direction']
                elif num_connections == 2:
                    data['direction'] = "%s/%s" % (entry['connection1_direction'], entry['connection2_direction'])

            else:
                data['connections'] = 0


            # print(data)
            state_change_data.append(data)

        previous_state = entry['agent_state']

    if print_output:
        print("AGENT STATE TIMES")
        for i in state_change_data:
            print(i)

    ### Get in-call states

    # filter and sort just the in_call_states
    in_call_states = list(filter(lambda d: 'agent_in_call_state' in d, snapshot_data))
    in_call_states = sorted(in_call_states, key=operator.itemgetter('agent_call_state_time'))

    for i, entry in enumerate(in_call_states):
        if i + 1 >= len(in_call_states):
            break

        if in_call_states[i]['agent_in_call_state'] != in_call_states[i + 1]['agent_in_call_state']:
            td = ((in_call_states[i + 1]['agent_call_state_time'] - in_call_states[i][
                'agent_call_state_time']).total_seconds())

            num_connections = entry['connections']

            data = {
                'agentid': entry['agentid'],
                'type': 'in_call',
                'state': entry['agent_in_call_state'],
                'state_time': in_call_states[i]['agent_call_state_time_str'],
                'duration': td,
                'connections': num_connections
            }

            if num_connections == 1:
                data['direction'] = entry['connection1_direction']
            elif num_connections == 2:
                data['direction'] = "%s/%s" % (entry['connection1_direction'], entry['connection2_direction'])

            # print(data)
            state_change_data.append(data)

        previous_state = entry['agent_state']

    if print_output:
        print("AGENT IN CALL STATE TIMES")
        for i in state_change_data:
            if i['type'] == 'in_call':
                print(i)

    state_change_data = sorted(state_change_data, key=operator.itemgetter('state_time'))

    return state_change_data, snapshot_data


def output_csv(filename, state_change_data, snapshot_data, append=False):
    global unique_keys

    state_change_file = "%s_states.csv" % filename

    if append:
        file_permissions = 'w+'
    else:
        file_permissions = 'w'

    with open(state_change_file, file_permissions, newline='') as csvfile:
        fieldnames = ['type', 'state', 'state_time', 'duration']
        writer = csv.DictWriter(csvfile, dialect='excel', fieldnames=fieldnames)
        if not append:
            writer.writeheader()
        for entry in state_change_data:
            writer.writerow(entry)

    snapshot_file = "%s_snapshots.csv" % filename

    unique_keys = []

    def get_unique(item):
        global unique_keys
        if isinstance(item, dict):
            for key, value in item.items():
                if key not in unique_keys:
                    unique_keys.append(key)
                get_unique(value)

    for field in snapshot_data:
        get_unique(field)

    with open(snapshot_file, file_permissions, newline='') as csvfile:
        writer = csv.DictWriter(csvfile, dialect='excel', fieldnames=unique_keys )
        if not append:
            writer.writeheader()
        for entry in snapshot_data:
            writer.writerow(entry)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Processing utility for Amazon Connect Streams getLogs().download() data',
        epilog="First load CCP and run `connect.getLogs.download()`. \n"
               "Then run this on that file: 'getLogs_stats.py --name 'Agent Alice'"
               " -input 'connect-getLogs.json' --output output --print'"
    )
    parser.add_argument(
        '-n',
        '--name',
        dest='name',
        default='default',
        help='The name of the agent these logs are for'
    )
    parser.add_argument(
        '--input',
        '-i',
        dest='json_file',
        help='the json file from connect getLogs().download'
    )
    parser.add_argument(
        '-o',
        '--output',
        dest='csv_output',
        default=False,
        help='the csv file to export data to without an extension'
    )
    parser.add_argument(
        '-p',
        '--print',
        dest='show_data',
        action='store_true',
        help='output the processed data to the screen'
    )
    parser.add_argument(
        '-a',
        '--append',
        dest='append',
        action='store_true',
        default=False,
        help='Append data to existing output files'
    )
    args = parser.parse_args()
    state_changes, snapshots = process_getLogs(args.json_file, args.name, args.show_data)
    if args.csv_output:
        output_csv(args.csv_output, state_changes, snapshots, args.append)
