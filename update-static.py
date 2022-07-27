import requests

IANA_TLDS = 'https://data.iana.org/TLD/tlds-alpha-by-domain.txt'


def main():
    response = requests.get(IANA_TLDS)
    assert response.status_code == 200 and len(response.content) > 0
    with open('tlds.txt', 'w') as f:
        for line in response.text.splitlines():
            line = line.strip()
            if len(line) == 0 or line.startswith('#'):
                continue
            f.write(line.lower()+'\n')

if __name__ == '__main__':
    main()

