import pandas as pd
import requests
from fake_useragent import UserAgent
import re
from datetime import datetime
from natasha import (
    Segmenter,
    MorphVocab,

    NewsEmbedding,
    NewsNERTagger,

    PER,
    NamesExtractor,

    Doc
)


def wall(owner):
    token = ""
    v = '5.131'
    ua = UserAgent()
    emb = NewsEmbedding()
    segmenter = Segmenter()
    morph_vocab = MorphVocab()
    ner_tagger = NewsNERTagger(emb)
    names_extractor = NamesExtractor(morph_vocab)
    now = datetime.now()
    phones_total, names_total, mail_total = [], [], []
    offset = -100
    while True:
        offset += 100
        r = requests.get(f'https://api.vk.com/method/wall.get?owner_id={owner}&count=100&offset={offset}&access_token={token}&v={v}', headers={'user-agent': ua.random}).json()['response']['items']
        for s in r:
            try:
                check = int(s['is_pinned'])
                if check == 1:
                    continue
            except KeyError:
                pass
            ts = int(s['date'])
            conv = datetime.utcfromtimestamp(ts)
            if (now - conv).days > 365 or len(r) < 100:
                df = pd.DataFrame({'Телефон': phones_total,
                                   'ФИО': names_total,
                                   'Почта': mail_total})
                return df
            text = s['text'].split('\n\n')
            for t in text:
                phones = re.findall(r'(\+7|8| ).*?(\d{3}).*?(\d{3}).*?(\d{2}).*?(\d{2})', t)
                if len(phones) != 0:
                    if phones[0][0] == ' ':
                        phone = ['8 ' + ' '.join(i).strip() for i in phones]
                    else:
                        phone = [(' '.join(i).strip()).replace('+7', '8') for i in phones]
                    mail = [i for i in t.split() if '@' in i and '.' in i and '#' not in i]
                    to_split = phone[0].split()[-1]
                    for_name = t.split(to_split)[-1]
                    doc = Doc(for_name)
                    doc.segment(segmenter)
                    doc.tag_ner(ner_tagger)
                    for span in doc.spans:
                        span.normalize(morph_vocab)
                    for span in doc.spans:
                        if span.type == PER:
                            span.extract_fact(names_extractor)
                    name = list({_.normal: _.fact.as_dict for _ in doc.spans if _.fact})
                    phones_total.append(', '.join(phone))
                    names_total.append(', '.join(name))
                    mail_total.append(', '.join(mail))


def main():
    df = pd.DataFrame()
    df.to_excel('output.xlsx')
    owners = [-166751173, -166779704, -112160203, -207216989]
    for k in range(len(owners)):
        with pd.ExcelWriter('output.xlsx', mode='a') as writer:
            wall(owners[k]).to_excel(writer, sheet_name=f'vk{k}', index=False)


if __name__ == "__main__":
    main()
