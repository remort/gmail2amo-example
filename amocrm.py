import logging
import os
import pathlib
from datetime import datetime
from json import JSONDecodeError
from typing import Optional, List, Dict, Any
from urllib import parse

import requests
from requests import Response
from requests.cookies import RequestsCookieJar

log = logging.getLogger("Amocrm API")
logging.basicConfig(level='INFO')

cookies = None


class Amo:
    DEFAULT_RESPONSIBLE_USER: str = '***@***.ru'

    def __init__(self, mailbox, responsible_user_login):
        self._mailbox: str = mailbox
        self._cookies: RequestsCookieJar = self._amo_auth()
        self._api_endpoint: str = 'https://***.amocrm.ru/api/v2/'
        self._responsible_user_id: str = self._get_responsible_user_id(responsible_user_login)
        self._attachments_dir = pathlib.Path('/mnt/amo-files')
        self._attachments_dir.mkdir(exist_ok=True)
        self._attachments_link = f'https://***'

    @staticmethod
    def _amo_auth() -> RequestsCookieJar:
        # TODO: обработка ошибок
        data: Dict[str, str] = {
            "USER_LOGIN": "***@***.ru",
            "USER_HASH": "***",
        }
        resp: Response = requests.post(url='https://***.amocrm.ru/private/api/auth.php?type=json', data=data)
        return resp.cookies

    def _make_request(
            self,
            method: str,
            http_method: str = 'get',
            data: Optional[Dict[str, Any]] = None,
            params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if http_method in ('get', 'options'):
            assert data is None

        if http_method in ['post', 'put']:
            assert data

        url = f'{self._api_endpoint}{method}'
        try:
            resp = requests.request(url=url, method=http_method, json=data, params=params, cookies=self._cookies)
        except:
            log.exception(
                f'Не удалось выполнить запрос к AMO CRM. API Method: {method}, data: {data}, params: {params}'
            )
            return None
        if resp.status_code not in [200, 204]:
            try:
                err = resp.json()
                if 'response' in err:
                    err = err['response'].get('error')
                elif 'detail' in err:
                    err = err['detail']
            except JSONDecodeError:
                err = resp.text

            log.error(f"AMO CRM вернул ошубку на запрос '{method}'. Текст ответа: {err}")

        # Пустой 204 ответ в АМО это "объект не найден"
        if resp.status_code == 204 and not resp.text:
            log.error(f'AMO responded with status code: {resp.status_code}')
            return None

        try:
            resp = resp.json()
            if method == 'contacts':
                return resp['_embedded']['items'][0] if resp['_embedded']['items'][0] else None
            if method == 'leads':
                return resp['_embedded']['items'] if resp['_embedded'].get('items') else None
            return resp
        except JSONDecodeError:
            log.exception(f"Не удалось распарсить ответ AMO. Ответ: {resp.text()}")

    def _get_responsible_user_id(self, responsible_user_login: str) -> int:
        account: Dict[str, Any] = self._make_request(
            method='account',
            params={
                'with': 'users,custom_fields',
                'free_users': 'Y',
            }
        )

        # Ищем ответственного среди юзеров АМО. Если не находим, ищем дефолтного ответственного.
        user: Dict[str, Any]
        for user in account['_embedded']['users'].values():
            if user['login'] == responsible_user_login:
                log.info("Найден ответственный юзер в АМО")
                return user['id']
        for user in account['_embedded']['users'].values():
            if user['login'] == self.DEFAULT_RESPONSIBLE_USER:
                log.info(f"Ответственный юзер не найден в АМО, ищем ID дефолтного: {self.DEFAULT_RESPONSIBLE_USER}")
                return user['id']

        raise RuntimeError(
            "В АМО не найден ни ответственный ни дефолтный ответственный юзеры. Занесение заявок невозможно."
        )

    def _get_contact(self, email: str) -> Optional[Dict[str, Any]]:
        resp: Dict[str, Any] = self._make_request(
            method='contacts',
            params={
                'query': email,
            }
        )

        return resp

    def _create_contact(self, contact: Dict[str, Any], responsible_user_id: str) -> Dict[str, Any]:
        contact_obj: Dict[str, Any] = {
            "name": contact["name"],
            "responsible_user_id": responsible_user_id,
            "created_by": responsible_user_id,
            "custom_fields": [
                {
                    "id": 343735,
                    "values": [{'value': contact['post']}]
                },
                {
                    "id": 343737,
                    "values": [{'value': contact['phone'], 'enum': 'WORK'}]
                },
                {
                    "id": 343739,
                    "values": [{'value': contact['email'], 'enum': 'WORK'}]
                },
                {
                    "id": 343743,
                    "values": [{'value': contact['skype'], 'enum': 'SKYPE'}]
                },
            ]
        }
        if contact.get('mobile'):
            contact_obj['custom_fields'].append({
                "id": 343737,
                "values": [{'value': contact['mobile'], 'enum': 'MOB'}]
            })
        if contact.get('home'):
            contact_obj['custom_fields'].append({
                "id": 343737,
                "values": [{'value': contact['home'], 'enum': 'HOME'}]
            })
        if contact.get('fax'):
            contact_obj['custom_fields'].append({
                "id": 343737,
                "values": [{'value': contact['fax'], 'enum': 'FAX'}]
            })

        return self._make_request(
            method='contacts',
            http_method='post',
            data={"add": [contact_obj]},
        )

    def _save_attach(self, attachment: Dict[str, str]) -> str:
        filename: str = attachment['name'].strip().replace(' ', '_')
        pathlib.Path(self._attachments_dir.as_posix(), filename).write_bytes(attachment['data'])
        return filename

    def _create_leads_with_notes(self, leads: Dict[str, Any], notes: Dict[str, Any]) -> bool:
        """
        Логика пакетного создания leads а затем notes ожидает что AMO возвращает созданные лиды в том же порядке
        в котором они были ей отправлены.
        """
        new_leads: Dict[str, Any] = self._make_request(
            method='leads',
            http_method='post',
            data={"add": leads},
        )
        if not new_leads:
            log.error(f"AMO вернула пустой ответ при создании лидов.")
            return False

        new_notes: List[Dict[str, Any]] = []
        for note in notes:
            note.update({
                "element_id": new_leads[notes.index(note)]['id'],
                "created_at": int(datetime.now().timestamp())
            })
            attachments: List[Optional[Dict[str, Any]]] = note.pop("attachments")

            # Складываем в лист notes на создание в АМО копию заметки, чтобы ниже добавить модифицированные копии notes
            # для всех аттачей, а не создавать каждый note заново (избавляемся от копипасты)
            new_notes.append(note.copy())
            for attachment in attachments:
                filename: str
                size: str
                filename: str = self._save_attach(attachment)
                # Используем тут старый объект note чтобы изменить у него тело и скопировать в new_notes как новый
                websafe_link: str = os.path.join(self._attachments_link, parse.quote_plus(filename))
                note['text'] = f"""Файл: {filename}\nСсылка: {websafe_link} ({attachment['size'] // 1024} Kb)"""
                new_notes.append(note.copy())

        resp = self._make_request(
            method='notes',
            http_method='post',
            data={"add": new_notes},
        )
        if not resp:
            log.error(f"AMO вернула пустой ответ при создании заметок к лидам.")
            return False
        return True

    def process_mails(self, mails: List[Dict[str, Any]]) -> bool:
        leads: List[Dict[str, Any]] = []
        notes: List[Dict[str, Any]] = []
        contact: Dict[str, Any]
        mail: Dict[str, Any]
        for mail in mails:
            contact = self._get_contact(mail['contact']['email'])
            if not contact:
                contact = self._create_contact(mail['contact'], self._responsible_user_id)

            leads.append({
                "name": mail['subject'],
                "contacts_id": contact['id'],
                "custom_fields": [{
                    "id": 531407,
                    "values": [{'value': self._mailbox}]
                }],
                "responsible_user_id": self._responsible_user_id,
            })

            notes.append({
                "text": mail['body'] if mail.get('body') else mail.get('html_body'),
                "attachments": mail['attachments'],
                "responsible_user_id": self._responsible_user_id,
                "created_by": self._responsible_user_id,
                "element_type": 2,
                "note_type": 4,
            })

        if leads:
            return self._create_leads_with_notes(leads, notes)

        return True
