import requests

cookies = {
    'bc-device-id': 'fd981878-faf4-43a3-a1ce-6b87948f3b6c',
    'BC-TOKEN': 'eyJhbGciOiJIUzI1NiIsImtpZCI6ImM0MzQ3NWY5LTVlNjMtNDFjZC1hMWNlLWJlMTM1NWZmODUwZSIsInR5cCI6IkpXVCJ9.eyJqdGkiOiI3ZWNjMzdhMS05MWY0LTQ5NjYtYjA3MS05OTNmMmJmOGJlODgiLCJyem4iOiJGUiIsImJybiI6IkJFVENMSUMiLCJwbHQiOiJERVNLVE9QIiwibG5nIjoiZnIiLCJ1bnYiOiJTUE9SVFMiLCJzaXQiOiJGUkZSIiwiZnB0IjoiZmQ5ODE4NzgtZmFmNC00M2EzLWExY2UtNmI4Nzk0OGYzYjZjIiwibmJmIjoxNzY5MTc2OTk4LCJleHAiOjE3Njk3ODE3OTgsImlzcyI6IkJFVENMSUMuRlIifQ.9D_CNmR3lIUDkpTZrun9yL2_PzBKGj3CA8GfziJG_KQ',
    'theme': 'light',
    'TCPID': '126151933355270501940',
    'TC_PRIVACY': '0%21010%7C19%7C5606%212%2C5%216%211769177040214%2C1769177040214%2C1784729040214%21',
    'TC_PRIVACY_CENTER': '2%2C5',
    '_gcl_au': '1.1.424578170.1769177040',
    '_scid': 'p5slgBlaBnUaHV7xaSG1wdqg4QMBs6Zv',
    '_ScCbts': '%5B%22243%3Bchrome.2%3A2%3A5%22%2C%22299%3Bchrome.2%3A2%3A5%22%5D',
    '_fbp': 'fb.1.1769177048871.590296656404793136',
    '_clck': 'ehtgpe%5E2%5Eg31%5E1%5E2214',
    'DATADOG_CORRELATION_ID': '2fff5e17-c8aa-4147-a0dd-d90eb25abce8',
    'BC-TIMEZONE': '{%22ianaName%22:%22Asia/Calcutta%22}',
    '_scid_r': 'oBslgBlaBnUaHV7xaSG1wdqg4QMBs6ZvJBnCHw',
    '_uetsid': 'ca11d280fa8f11f0a6493bf55528b43b',
    '_uetvid': '611a4790f86411f0852d557c2f3bf65a',
    '_clsk': 'rptgbg%5E1769418818950%5E1%5E1%5Ed.clarity.ms%2Fcollect',
    '_dd_s': 'aid=03a3d112-3286-450b-8ab7-13e58bc864a3&rum=0&expire=1769419721964&logs=1&id=a74a621b-7824-4971-8349-d4db9950c8d8&created=1769418791503',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'referer': 'https://www.betclic.fr/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    # 'cookie': 'bc-device-id=fd981878-faf4-43a3-a1ce-6b87948f3b6c; BC-TOKEN=eyJhbGciOiJIUzI1NiIsImtpZCI6ImM0MzQ3NWY5LTVlNjMtNDFjZC1hMWNlLWJlMTM1NWZmODUwZSIsInR5cCI6IkpXVCJ9.eyJqdGkiOiI3ZWNjMzdhMS05MWY0LTQ5NjYtYjA3MS05OTNmMmJmOGJlODgiLCJyem4iOiJGUiIsImJybiI6IkJFVENMSUMiLCJwbHQiOiJERVNLVE9QIiwibG5nIjoiZnIiLCJ1bnYiOiJTUE9SVFMiLCJzaXQiOiJGUkZSIiwiZnB0IjoiZmQ5ODE4NzgtZmFmNC00M2EzLWExY2UtNmI4Nzk0OGYzYjZjIiwibmJmIjoxNzY5MTc2OTk4LCJleHAiOjE3Njk3ODE3OTgsImlzcyI6IkJFVENMSUMuRlIifQ.9D_CNmR3lIUDkpTZrun9yL2_PzBKGj3CA8GfziJG_KQ; theme=light; TCPID=126151933355270501940; TC_PRIVACY=0%21010%7C19%7C5606%212%2C5%216%211769177040214%2C1769177040214%2C1784729040214%21; TC_PRIVACY_CENTER=2%2C5; _gcl_au=1.1.424578170.1769177040; _scid=p5slgBlaBnUaHV7xaSG1wdqg4QMBs6Zv; _ScCbts=%5B%22243%3Bchrome.2%3A2%3A5%22%2C%22299%3Bchrome.2%3A2%3A5%22%5D; _fbp=fb.1.1769177048871.590296656404793136; _clck=ehtgpe%5E2%5Eg31%5E1%5E2214; DATADOG_CORRELATION_ID=2fff5e17-c8aa-4147-a0dd-d90eb25abce8; BC-TIMEZONE={%22ianaName%22:%22Asia/Calcutta%22}; _scid_r=oBslgBlaBnUaHV7xaSG1wdqg4QMBs6ZvJBnCHw; _uetsid=ca11d280fa8f11f0a6493bf55528b43b; _uetvid=611a4790f86411f0852d557c2f3bf65a; _clsk=rptgbg%5E1769418818950%5E1%5E1%5Ed.clarity.ms%2Fcollect; _dd_s=aid=03a3d112-3286-450b-8ab7-13e58bc864a3&rum=0&expire=1769419721964&logs=1&id=a74a621b-7824-4971-8349-d4db9950c8d8&created=1769418791503',
}

response = requests.get('https://www.betclic.fr/football-sfootball', cookies=cookies, headers=headers)