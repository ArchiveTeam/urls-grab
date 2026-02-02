import os
import re
import traceback
import typing

import requests

IANA_TLDS = 'https://data.iana.org/TLD/tlds-alpha-by-domain.txt'
USER_AGENTS = 'https://www.whatismybrowser.com/guides/the-latest-user-agent/firefox'
PUBLIC_SUFFIX_LIST = 'https://raw.githubusercontent.com/publicsuffix/list/master/public_suffix_list.dat'
SPECIAL_TLDS = {'arpa', 'gov', 'mil', 'museum', 'edu', 'org'}
SPECIAL_LABELS = (
    {
        "academy",
        "education",
        "edu",
        "ed",
        "institute",
        "sch",
        "school",
        "schools",
        "k12",
        "college",
        "university",
        "uni",
        "univ",
        "student",
        "schule",
    }
    | {
        "gov",
        "gob",
        "go",
        "gouv",
        "govt",
        "gobierno",
        "gouvernement",
        "government",
        "mil",
        "army",
        "navy",
        "airforce",
        "lg",
        "gub",
        "state",
        "county",
        "commune",
        "muni",
        "municipal",
        "municipio",
        "council",
        "parliament",
        "pref",
        "prefecture",
        "province",
        "region",
        "department",
        "district",
        "adm",
        "dep",
        "kommune",
    }
    | {
        "org",
        "or",
        "ngo",
        "ong",
        "charity",
        "foundation",
        "giving",
        "gives",
        "aid",
        "asso",
        "assoc",
        "association",
        "assn",
    }
    | {
        "int",
        "intl",
        "international"
    }
    | {
        "health",
        "healthcare",
        "hospital",
        "hosp",
        "clinic",
        "clinique",
        "care",
        "doctor",
        "dentist",
        "pharmacy",
        "med",
        "medecin",
        "medicina",
        "vet",
        "salud",
        "nhs",
        "police",
        "fire",
        "ambulance",
        "rescue",
        "emergency",
        "safety",
        "security",
        "mod",
    }
)
SPECIAL_LABELS_NOT_ENDING = {"ac", "city", "gop"}


def write_file(filename: str, data: typing.Union[str, typing.List[str]]) -> int:
    if type(data) is list:
        data = '\n'.join(data) + '\n'
    if type(data) is str:
        data = bytes(data, 'utf8')
    result = None
    temp_filename = filename + '.bak'
    if os.path.isfile(temp_filename):
        if not os.path.isfile(filename):
            os.rename(temp_filename, filename)
        else:
            os.remove(temp_filename)
    if os.path.isfile(filename):
        os.rename(filename, temp_filename)
    try:
        with open(filename, 'wb') as f:
            result = f.write(data)
    except Exception:
        traceback.print_exc()
        if os.path.isfile(filename):
            os.remove(filename)
    finally:
        if not os.path.isfile(filename) \
            and os.path.isfile(temp_filename):
            os.rename(temp_filename, filename)
        if os.path.isfile(filename) \
            and os.path.isfile(temp_filename):
            os.remove(temp_filename)
    return result


def update_tlds() -> typing.List[str]:
    response = requests.get(IANA_TLDS)
    assert response.status_code == 200 and len(response.content) > 0
    tlds = ['onion']
    for line in response.text.splitlines():
        line = line.strip()
        if len(line) == 0 or line.startswith('#'):
            continue
        tlds.append(line.lower())
    write_file('static-tlds.txt', tlds)
    return tlds


def update_uas() -> typing.List[str]:
    response = requests.get(USER_AGENTS)
    assert response.status_code == 200 and len(response.content) > 0
    user_agents = set()
    for h2, t in re.findall(r'<h2>([^<]+)</h2>\s*(<table.+?</table>)', response.text, re.S):
        h2 = h2.lower()
        if 'firefox' in h2 and 'desktop' in h2:
            for ua in re.findall(r'<span class="code">([^<]+)</span>', t):
                user_agents.add(ua.strip())
            break
    if len(user_agents) > 0:
        user_agents = sorted(user_agents)
        write_file('user-agents.txt', user_agents)
    else:
        user_agents = []
    return user_agents


def update_outlinks_domains() -> typing.List[str]:
    lines = set()
    lines |= SPECIAL_TLDS
    with open('static-extract-outlinks-domains.txt', 'r') as f:
        for line in f:
            line = line.strip()
            if len(line) == 0:
                continue
            if '.' not in line:
                continue
            lines.add(line.lower())
    response = requests.get(PUBLIC_SUFFIX_LIST)
    assert response.status_code == 200 and len(response.text) > 100000
    for line in response.text.splitlines():
        line = line.strip().lower()
        if len(line) == 0 or line.startswith('//'):
            continue
        line = line.strip('*.!').split('.')
        if any(label in SPECIAL_LABELS for label in line) \
            or any(label in SPECIAL_LABELS_NOT_ENDING for label in line[:-1]):
            lines.add('.'.join(line))
            continue
    for tld in update_tlds():
        if tld in (SPECIAL_LABELS | SPECIAL_TLDS) and tld not in SPECIAL_LABELS_NOT_ENDING:
            lines.add(tld)
    lines = sorted(lines)
    write_file('static-extract-outlinks-domains.txt', lines)
    return lines


def dedup_and_sort(filename: str):
    lines = []
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if len(line) > 0:
                lines.append(line)
    os.rename(filename, filename+'.bak')
    with open(filename, 'w') as f:
        f.write('\n'.join(sorted(set(lines))))
    os.remove(filename+'.bak')

if __name__ == '__main__':
    print('Updating TLDs.')
    update_tlds()
    print('Updating user-agents.')
    update_uas()
    print('Updating outlinks list.')
    update_outlinks_domains()
    print('Processing filter patterns.')
    dedup_and_sort('static-filter-discovered.txt')

