from __future__ import print_function

import requests


def main():
    url = "http://131.169.55.170/filewriter/api/1.5.0/files"

    session = requests.session()
    r = session.get(url)
    r.raise_for_status()
    print("text")
    print(r.text)
    print("json")
    print(r.json())


if __name__ == "__main__":
    main()
