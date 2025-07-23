import json
import os
import re
import shutil
import sys
from argparse import ArgumentParser
from copy import copy
from pathlib import Path
from uuid import uuid4

from tqdm import tqdm

max_int = (1 << 32) - 1


def read_grammar_rules(file_path):
    with open(file_path, 'r') as lines:
        probabilities = {}

        try:
            cumulative_prob = 0.0
            for line in lines:
                seq, prob = line.rstrip('\r\n').rsplit('\t', maxsplit=1)
                prob = float(prob)
                if prob not in probabilities:
                    probabilities[prob] = []
                probabilities[prob].append(seq)
                cumulative_prob += prob
        except Exception as e:
            print(f'error while processing {file_path}: {str(e)}')
            raise e

        # TODO: triggered on cumulative probability 0.982157254932863 != 1.0
        # if not math.isclose(cumulative_prob, 1.0, rel_tol=1e-5):

        #  TODO: disabled for custom, triggered on YEARS
        # if not math.isclose(cumulative_prob, 1.0, rel_tol=0.05):
        #     raise Exception(f'cumulative probability {cumulative_prob} != 1.0 for: {file_path}')

        return probabilities


def merge_nearest(probabilities, num_bins, max_delta):
    def ddelta(high, low):
        if high < low:
            raise ValueError(f'{high} > {low}')
        return (high - low) / high

    if len(probabilities) <= 1:
        return probabilities

    merged = [(prob, vars) for prob, vars in probabilities.items()]

    deltas = []
    for idx in range(len(merged) - 1):
        deltas.append([ddelta(merged[idx][0], merged[idx + 1][0]), idx])
    deltas.sort(key=lambda entry: entry[0])

    while len(merged) > 1:
        next_delta, next_idx = deltas[0]
        if num_bins != max_int:
            if len(merged) <= num_bins:
                if next_delta > max_delta:
                    break
        else:
            if next_delta > max_delta:
                break

        l_prob, r_prob = merged[next_idx][0], merged[next_idx + 1][0]
        l_vars, r_vars = merged[next_idx][1], merged[next_idx + 1][1]

        new_prob = (l_prob * len(l_vars) + r_prob * len(r_vars)) / (len(l_vars) + len(r_vars))
        new_vars = (l_vars + r_vars)

        new_deltas = []
        for delta, idx in deltas:
            if idx == next_idx - 1 or idx == next_idx or idx == next_idx + 1:
                continue
            if idx > next_idx:
                idx -= 1
            new_deltas.append((delta, idx))

        if next_idx > 0:
            new_deltas.append((ddelta(merged[next_idx - 1][0], new_prob), next_idx - 1))

        if next_idx + 2 < len(merged):
            new_deltas.append((ddelta(new_prob, merged[next_idx + 2][0]), next_idx))

        merged = merged[:next_idx] + [(new_prob, new_vars)] + merged[next_idx + 2:]
        new_deltas.sort(key=lambda entry: entry[0])
        deltas = new_deltas

    merged.sort(key=lambda entry: entry[0], reverse=True)

    #  TODO: disabled for custom, triggered on YEARS
    # cumulative_prob = sum(prob * len(terminals) for prob, terminals in merged)
    # if not math.isclose(cumulative_prob, 1.0, rel_tol=1e-5):
    #     raise Exception(f'cumulative probability {cumulative_prob} != 1.0')

    if len(probabilities) != len(merged):
        print(f'Reduced number of bins from {len(probabilities)} to {len(merged)}')

    return dict(merged)


# calculate num variables after they are read and possibly extended, e.g.:
#   len(D4O1) == 2, because it has a D variable and an O variable
#   len(D1O1D1O1Y1) == 5, because it has 2 D variables, 2 O variables and a Y variable
#   len(A1D1A1) == 3 or 5, because it has an A variable and a D variable,
#                          but it will be extended by a casing variable C for each alpha A
#                          (so under the hood looks like A1C1D1A1C1)
def num_variables_in_base_struct(base_struct, include_extension=False):
    variables = re.split(r'(?<=\d|\D)(?=\D)', base_struct)

    num_variables = len(variables)
    if include_extension:
        num_variables += sum(1 for variable in variables if variable.startswith('A'))

    return num_variables


def size_largest_variable(base_struct):
    variables = re.split(r'(?<=\d|\D)(?=\D)', base_struct)
    variables = [variable for variable in variables if len(variable) > 1]  # filter e.g. E

    if not variables:
        return 0

    return max(int(variable[1:]) for variable in variables)


def generated_password_length(base_struct):
    variables = re.split(r'(?<=\d|\D)(?=\D)', base_struct)
    variables = [variable for variable in variables if len(variable) > 1]
    # others can be variable length, but taking at least as 1 is sane
    # variables = [variable for variable in variables if variable[0] in 'ADOK']

    return sum(int(variable[1:]) for variable in variables)


if __name__ == '__main__':
    args = ArgumentParser()

    args.add_argument('input', help='')
    args.add_argument('output', help='')
    args.add_argument('-b', '--max-bins', type=int, default=max_int,
                      help='Merge closest probability terminals of a terminal set until there are only <max-bins> of different probabilities left.')
    args.add_argument('-m', '--max-merge-delta', type=float, default=0.0,
                      help='Merge terminals of a terminal set which differ less than <max-merge-delta> percent.')
    args.add_argument('-t', '--max-truncate-delta', type=float, default=0.0,
                      help='Remove terminals with a probability less than <max-truncate-delta> percent of the maximum probability in the set. ')  # default=0.001
    args.add_argument('-g', '--max-grammar-length', type=int, default=max_int,
                      help='Remove base structures for which the number of variables exceed <max-grammar-length>.')
    args.add_argument('-v', '--max-variable-length', type=int, default=max_int,
                      help='Remove base structures containing variables longer than given length, and remove the terminals themselves.')
    args.add_argument('-d', '--doubled-base-structures', type=float, default=0.0,
                      help='Create doubled base structures, with probability distributed between the original and the doubled proportionate to given fraction.')
    args.add_argument('-p', '--max-password-length', type=float, default=max_int,
                      help='Remove base structures which would generate passwords longer than <length>.')
    # this means do we include e.g. capitalization variables, which do not show in the base structure:
    #   len(D4O1) == 2,        because it has a D variable and an O variable
    #   len(D1O1D1O1Y1) == 5,  because it has 2 D variables, 2 O variables and a Y variable
    #   len(A1D1A1) == 3 or 5, because it has an A variable and a D variable,
    #                          but it will be extended by a casing variable C for each alpha A
    #                          (so under the hood looks like A1C1D1A1C1)
    args.add_argument('-x', '--include-casing-in-grammar-length', action='store_true', default=False, help='Also count the C variables which are automatically added for each A in the grammar length.')

    # hacks
    args.add_argument('--remove-base-structure-if', default=None, help='Remove base structures which match given predicate.')

    args = args.parse_args()

    input = Path(args.input)
    output = Path(args.output)
    max_bins = args.max_bins
    max_merge_delta = args.max_merge_delta
    max_truncate_delta = args.max_truncate_delta
    max_grammar_length = args.max_grammar_length
    max_variable_length = args.max_variable_length
    include_casing_in_grammar_length = args.include_casing_in_grammar_length
    doubled_base_structures = args.doubled_base_structures
    max_password_length = args.max_password_length
    remove_base_structure_if = args.remove_base_structure_if

    if Path.exists(output):
        # print(f'Output directory {output} already exists, exiting...')
        print(f'Output directory {output} already exists, overwriting...')
        # exit(1)
        shutil.rmtree(output)
    os.mkdir(output)

    for root, _, files in os.walk(input):
        print(f'Processing {root}...', file=sys.stderr)
        for file in tqdm(files, unit=' files', bar_format='{l_bar}{bar}', ncols=80):
            file_path = os.path.join(root, file)

            rel_dir = os.path.relpath(root, input)
            rel_file = os.path.join(rel_dir, file)

            out_file = Path(os.path.join(output, rel_file))
            out_file.parent.mkdir(parents=True, exist_ok=True)

            # we should update the UUID to something new
            if file == 'config.ini':
                with open(file_path, 'r') as config_in:
                    with open(out_file, 'w') as config_out:
                        for line in config_in:
                            if line.startswith('uuid = '):
                                config_out.write(f'uuid = {uuid4()}\n')
                            # remove the terminal sets which are too long
                            elif line.startswith('filenames = '):
                                print(f'Reduces filenames in config:')
                                filenames = json.loads(line.split(' = ')[1])
                                print(f'  {filenames}')
                                filenames = [name for name in filenames if not re.match(r'\d+\.txt', name) or (int(name.split('.')[0]) <= max_variable_length and int(name.split('.')[0]) <= max_password_length)]
                                print(f'  {filenames}')
                                config_out.write(f'filenames = {json.dumps(filenames)}\n')
                            else:
                                config_out.write(line)

            elif rel_dir not in ['Grammar', 'Capitalization', 'Omen', '.']:
                if re.match(r'\d+\.txt', file):
                    length = int(file.split('.')[0])
                    if length > max_variable_length:
                        print(f'Too large variables, skipping: {file_path}')
                        continue
                    if length > max_password_length:
                        print(f'Longer than max password length, skipping: {file_path}')
                        continue

                print(f'Processing {file_path}')
                # this only joins entries in terminal sets, so resulting set of terminals should remain the same
                if (max_bins != max_int or max_merge_delta != 0.0):
                    grammar = read_grammar_rules(file_path)
                    merged = merge_nearest(grammar, max_bins, max_merge_delta)

                    with open(out_file, 'w') as out_file:
                        for probability, terminals in merged.items():
                            for terminal in terminals:
                                if len(terminal) > max_variable_length:
                                    # not yet supported, because if file would be empty, would have to update base structs
                                    # for now, be lazy
                                    raise Exception(f'filtering non-basic terminal sets on length not yet supported: {terminal}')
                                out_file.write(f'{terminal}\t{probability}\n')
                else:
                    shutil.copy(file_path, out_file)

            # this reduces grammar entries, so resulting set of terminals will probably be reduced
            elif rel_dir in ['Grammar', 'Capitalization'] and (max_truncate_delta != 0.0 or max_grammar_length != max_int or doubled_base_structures != 0 or max_password_length != max_int or remove_base_structure_if is not None):
                grammar = read_grammar_rules(file_path)

                max_prob, min_prob = 0.0, float('inf')

                should_remove = eval(f'lambda base_structure: {remove_base_structure_if}') if remove_base_structure_if else lambda _: False

                if rel_dir == 'Grammar':
                    outputs = {}
                    for probability, base_structures in grammar.items():
                        max_prob, min_prob = max(max_prob, probability), min(min_prob, probability)

                        if probability / max_prob < max_truncate_delta:
                            break

                        for base_structure in base_structures:
                            if should_remove(base_structure):
                                continue

                            length = num_variables_in_base_struct(base_structure, include_casing_in_grammar_length)
                            if length > max_grammar_length:
                                continue

                            size = size_largest_variable(base_structure)
                            if size > max_variable_length:
                                continue

                            password_length = generated_password_length(base_structure)
                            if password_length > max_password_length:
                                continue

                            outputs[base_structure] = probability

                    # TODO: is copy necessary
                    if doubled_base_structures > 0.0:
                        for base_structure, probability in copy(outputs).items():
                            double_base_structure = base_structure + base_structure
                            if double_base_structure not in outputs:
                                length = num_variables_in_base_struct(double_base_structure, include_casing_in_grammar_length)
                                if length <= max_grammar_length:
                                    outputs[base_structure] = probability * (1.0 - doubled_base_structures)
                                    outputs[double_base_structure] = probability * doubled_base_structures

                    outputs = [(bs, p) for bs, p in outputs.items()]
                    sorted(outputs, key=lambda x: x[1], reverse=True)

                    with open(out_file, 'w') as out_file:
                        for base_structure, probability in outputs:
                            out_file.write(f'{base_structure}\t{probability}\n')
                else:
                    with open(out_file, 'w') as out_file:
                        for probability, base_structures in grammar.items():
                            max_prob, min_prob = max(max_prob, probability), min(min_prob, probability)

                            if probability / max_prob < max_truncate_delta:
                                break

                            for base_structure in base_structures:
                                out_file.write(f'{base_structure}\t{probability}\n')

            else:
                shutil.copy(file_path, out_file)
