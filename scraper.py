import json
import csv
import os
from pathlib import Path
from pprint import pprint
import re

import requests
from bs4 import BeautifulSoup

class Issue:

    TITLE_REGEX = re.compile("<title>(.*)</title>", re.I | re.M | re.S)
    BASE_URL = "http://bugs.python.org/issue{0}"

    def __init__(self, issue_id):
        self.issue_id = issue_id
        self.filename = os.path.join("data", issue_id)
        self.success = False

        my_file = Path(self.filename)
        if not my_file.is_file():
            self.download_issue()

        if self.success:
            with open(self.filename) as f:
                self.content = f.read()
                self.soup = BeautifulSoup(self.content, 'html.parser')

    def download_issue(self):
        print("Downloading issue : {0}".format(self.issue_id))
        url = self.BASE_URL.format(self.issue_id)
        response = requests.get(url, verify=False)
        if response.status_code == 200:
            with open(self.filename, 'w+') as f:
                f.write(response.text)
                self.success = True

    def get_title(self):
        match = self.TITLE_REGEX.search(self.content)
        if match:
            return match.group(1).strip().replace("\n", "")
        return None

    def get_component(self):
        match = re.search('Component.*?<td>(.*?)</td>', self.content, re.I | re.M | re.S)
        if match:
            return list(map(lambda x: x.strip(), match.group(1).strip().split(",")))
        return None

    def get_version(self):
        match = re.search('Version.*?<td>(.*?)</td>', self.content, re.I | re.M | re.S)
        if match:
            return list(map(lambda x: x.strip(), match.group(1).strip().split(",")))
        return None

    def get_authors(self):
        return [item.text.strip() for item in self.soup.findAll('th') if "Author:" in item.text]

    def get_dates(self):
        return [item.text.strip() for item in self.soup.findAll('th') if "Date:" in item.text]

    def get_comments(self):
        return [item.text.strip() for item in self.soup.findAll('td', attrs={'class': 'content'})]

    def parse_content(self):
        data = {}
        data['component'] = self.get_component()
        data['version'] = self.get_version()
        data['title'] = self.get_title()
        data['content'] = []
        authors = self.get_authors()
        dates = self.get_dates()
        comments = self.get_comments()

        for author, date, comment in zip(authors, dates, comments):
            data['content'].append({'author': author, 'date': date, 'comment': comment})

        return data

    def write_to_file(self, data):
        with open(self.filename + ".json", 'w') as f:
            json.dump(data, f)

if __name__ == "__main__":
    with open('python_bugs.csv', 'r') as f:
        reader = csv.DictReader(f)
        for issue_id in range(12270, 34000):
            issue_id = str(issue_id)
            issue = Issue(issue_id)
            if issue.success:
                print("Processing issue : ", issue_id)
                data = issue.parse_content()
                issue.write_to_file(data)
            else:
                print("Invalid issue : ", issue_id)
