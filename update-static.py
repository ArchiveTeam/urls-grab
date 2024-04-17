import re

import requests

IANA_TLDS = 'https://data.iana.org/TLD/tlds-alpha-by-domain.txt'
USER_AGENTS = 'https://www.useragents.me/'


def update_tlds():
    response = requests.get(IANA_TLDS)
    assert response.status_code == 200 and len(response.content) > 0
    with open('static-tlds.txt', 'w') as f:
        f.write('onion\n')
        for line in response.text.splitlines():
            line = line.strip()
            if len(line) == 0 or line.startswith('#'):
                continue
            f.write(line.lower()+'\n')


def update_uas():
    response = requests.get(USER_AGENTS)
    assert response.status_code == 200 and len(response.content) > 0
    user_agents = {}
    data = re.search('<h2 id="latest-windows-desktop-useragents">(.+?)<h2', response.text, re.S).group(1)
    for browser, version, agent in re.findall(r'<td>([0-9a-zA-Z]+) ([0-9][0-9\.]+)[^<]+</td>\s*<td>\s*<div class="input-group">\s*<textarea class="form-control ua-textarea">([^<]+?)</textarea>\s*</div>\s*</td>', data):
        browser = browser.lower()
        if browser not in user_agents:
            user_agents[browser] = {}
        version += '.' + str(-len(user_agents[browser]))
        version = tuple(int(s) for s in version.split('.'))
        user_agents[browser][version] = agent
    for browser, versions in user_agents.items():
        user_agents[browser] = sorted(versions.items(), key=lambda x: x[0], reverse=True)[0][1]
    if len(user_agents) > 0:
        with open('user-agents.txt', 'w') as f:
            f.write('\n'.join(sorted(user_agents.values())))

if __name__ == '__main__':
    update_tlds()
    update_uas()

