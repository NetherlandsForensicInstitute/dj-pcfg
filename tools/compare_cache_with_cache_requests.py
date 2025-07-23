import sys
from datetime import datetime, timedelta

if __name__ == '__main__':
    rule_uid = sys.argv[1]
    # on cache server, if modified time is not too long ago...:
    #   ../cache$ ls -alh -t
    ls_output = sys.argv[2]
    # on client log(s) directory:
    #   ../logs$ find -type f -name '*log' -exec sh -c 'echo {} && grep -B 1 -i receiving {}' \;
    grep_output = sys.argv[3]

    cache_files = []
    with open(ls_output, 'r') as input:
        for line in input:
            if rule_uid in line:
                line = line.rstrip()
                try:
                    *_, month, day, time, name = line.split(' ')
    
                    uid, offset = name.split('_')
                    date = datetime.strptime(f'{month}{day}{time}', '%b%d%H:%M')
    
                    cache_file = (uid, int(offset), date)
                    cache_files.append(cache_file)
                except ValueError:
                    pass

    cache_files.sort(key=lambda x: x[2])  # sort by
    # print(cache_files)

    cache_requests = []
    with open(grep_output, 'r') as input:
        request = None
        for line in input:
            line = line.rstrip()
            try:
                date, time, *_, uid, _, _, offset = line.split(' ')
                date = datetime.strptime(f'{date} {time}', '%Y-%m-%d %H:%M:%S.%f')
                date = date + timedelta(hours=2)
                date = date.replace(year=cache_files[0][2].year)

                if 'Requesting' in line:
                    request = (uid, int(offset), date)
                if 'Receiving' in line:
                    cache_requests.append((request, (uid, int(offset), date)))
                    request = None
            except ValueError:
                pass

    cache_requests.sort(key=lambda x: x[1][2])  # sort by received time
    # print(cache_requests)

    for request, response in cache_requests:
        req_uid, req_off, req_time = request
        res_uid, res_off, res_time = response

        if req_uid != res_uid:
            raise Exception(f'{req_uid} != {res_uid}')

        if req_uid != rule_uid:
            continue

        if not any(offset == res_off for _, offset, _ in cache_files):
            print(f'[UNDETERMINED] cache {res_uid}_{res_off} file has been deleted')
            continue

        prev = None
        for cache_file in cache_files:
            *_, time = cache_file
            if time > res_time:
                if not prev:
                    print(f'[UNDETERMINED] cache has no file anymore before {res_time}')
                    break
                uid, offset, time = prev
                if offset == res_off:
                    print(f'[OK] req: {req_off} res: {res_off}')
                else:
                    if time == req_time.replace(second=0, microsecond=0):
                        print(f'[OK (PROBABLY)] req: {req_off} res: {res_off}')
                    else:
                        print(f'[POSSIBLE ERROR] req: {req_off} res: {res_off} should be: {offset} (req_time={req_time}, res_time={res_time}, file_time={time})')
                break
            prev = cache_file
        else:
            uid, offset, time = prev
            if offset == res_off:
                print(f'[OK] req: {req_off} res: {res_off}')
            else:
                if time == req_time.replace(second=0, microsecond=0):
                    print(f'[OK (PROBABLY)] req: {req_off} res: {res_off}')
                else:
                    print(
                        f'[POSSIBLE ERROR] req: {req_off} res: {res_off} should be: {offset} (req_time={req_time}, res_time={res_time}, file_time={time})')