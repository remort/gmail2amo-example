#!/usr/bin/python3

import argparse
import logging
import time
from base64 import urlsafe_b64decode
from binascii import Error as BinasciiError
from multiprocessing import Pool
from pprint import PrettyPrinter
from typing import Optional, Tuple, List, Dict, Any, Union, Generator

from bs4 import BeautifulSoup
from googleapiclient.errors import HttpError

from amocrm import Amo
from classification_model import SGDClassificator
from google_api_utils import get_service, Resource, get_labels, USER_ID

log = logging.getLogger("Mail sorter")
logging.basicConfig(level='INFO')
logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)

clf: SGDClassificator = SGDClassificator()
service: Resource = get_service()
labels: Dict[str, str] = get_labels(service)
pp: PrettyPrinter = PrettyPrinter(indent=4)


def main():
    """Запускает цикл сбора и обработки входящих писем."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-j',
        '--jobs',
        help="Количество воркеров для одновременной обработки писем.",
        type=int,
        default=4,
    )
    parser.add_argument(
        '-t',
        '--timeout',
        help="Кол-во секунд перед следущей итерацией получения и обработки писем.",
        type=int,
        default=60,
    )
    parser.add_argument(
        '-u',
        '--responsible-user',
        help="Имэйл юзера в AMO CRM на которого будут создаваться заявки, н-р: ***@***.ru.",
        type=str,
        default='***@***.ru',
    )
    jobs: int = parser.parse_args().jobs
    timeout: int = parser.parse_args().timeout
    responsible_user: str = parser.parse_args().responsible_user
    amo = Amo('***@***.ru', responsible_user)

    while True:
        messages: List[Tuple[Any, Resource]] = get_messages()
        log.info(f"Найдено {len(messages)} новых сообщений")
        msgs: List[Dict[str, Any]] = list()
        try:
            if 0 < len(messages) < jobs:
                current_jobs: int = len(messages)
            else:
                current_jobs = jobs

            with Pool(processes=current_jobs) as pool:
                msg: Dict[str, Any]
                lead: bool
                for msg, lead in pool.map(task, messages):
                    if lead:
                        msgs.append(msg)

            log.info(f'Входящая почта обработана. Найдено {len(msgs)} заявок.')

            # Распараллеливать работу с АМО АПИ не надо т.к. у них суровые лимиты: 7 запросов в секунду.
            # Каждое создание лида это как минимум 2 запроса - лид и заметка. Если аттачей много - много заметок.
            # Т.о. в process_mails создаем лиды и заметки пакетно, экономя кол-во запросов к АМО АПИ и время создания.
            status: bool = amo.process_mails(msgs)
            if not status:
                log.info('Не удалось занести заявки в АМО.')
            log.info('Заявки в АМО занесены.')

            time.sleep(timeout)
        except KeyboardInterrupt:
            log.info('Цикл обработки прерван пользователем.')
            return


def get_messages():
    """Запрашивает список непрочитанных писем в ящике."""
    page_token: Optional[str] = None
    messages: List[Tuple[Any, Resource]] = list()
    try:
        while True:
            response: Dict[str, Any] = service.users().messages() \
                .list(userId=USER_ID, labelIds=['UNREAD'], pageToken=page_token).execute()

            if 'messages' in response:
                messages.extend([(x['id'], service) for x in response['messages']])

            if 'nextPageToken' in response:
                page_token = response['nextPageToken']
            else:
                break

    except HttpError:
        log.exception('Ошибка получения списка писем из ящика.')
    except KeyboardInterrupt:
        log.exception('Прервано пользователем на запросе новых писем в ящике.')
    return messages


def task(args) -> Tuple[Dict[str, Any], bool]:
    """
    Каждый таск запускается параллельно. Получает сообщение с gmail, парсит его, и метит как прочитанное.
    Возвращает распарсенное сообщение.
    Обработка 50 непрочитанных сообщений в gmail ящике в параллели занимает 14 секунд против 50-и сек. в простом цикле.
    """
    try:
        message: Dict[str, Any]
        service: Resource
        message, service = args
        msg: Dict[str, Any] = service.users().messages().get(userId=USER_ID, id=message).execute()
        msg = get_msg(msg, service)

        label_body: Dict[str, List[str]] = {
            'removeLabelIds': [labels['UNREAD']],
        }

        msg_text_content: str = msg['subject'] + msg['body'] + html2text(msg['html'])

        if clf.get_prediction(msg_text_content) == 0:
            log.info('Заявка не обнаружена')
            label_body['addLabelIds'] = [labels['Не заявка']]
            lead = False
        else:
            log.info('Обнаружена заявка')
            label_body['addLabelIds'] = [labels['Заявка']]
            lead = True

        service.users().messages().modify(userId=USER_ID, id=message, body=label_body).execute()
        log.debug(f'Письмо {message} помечено как прочитанное на сервере.')
        return msg, lead

    except KeyboardInterrupt:
        log.info('Таск обработки письма был прерван пользователем прямо во время своего выполнения!')


def html2text(html: str) -> str:
    """Избавляется от HTML в теле письма и возвращает только текст. Используется если у письма есть HTML контент."""
    if not html:
        return ''

    soup: BeautifulSoup = BeautifulSoup(html, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text: str = soup.get_text()
    lines: Generator[str, None, None] = (line.strip() for line in text.splitlines())
    chunks: Generator[str, None, None] = (phrase.strip() for line in lines for phrase in line.split("  "))

    return '\n'.join(chunk for chunk in chunks if chunk)


def get_sender(from_header: str) -> Tuple[str, str]:
    """Возвращает имя и адрес получателя из заголовка From. Имени может и не быть"""
    sender_name: str
    sender_address: str
    if ' ' in from_header:
        sender_name, sender_address = from_header.rsplit(' ', 1)
    else:
        sender_name = ''
        sender_address = from_header

    if sender_address.startswith('<') and sender_address.endswith('>'):
        sender_address = sender_address[1:-1]

    if not sender_name:
        sender_name = sender_address

    return sender_name, sender_address


def get_msg(message: Dict[str, Any], service: Resource):
    """Подготавливает письмо для экспорта в Amo CRM."""
    payload: Dict[str, Any] = message.get('payload')

    sender_name: str
    sender_address: str
    sender_name, sender_address = get_sender(get_header(payload.get('headers'), 'From'))
    subject: str = get_subject(payload)
    text_body: str = get_body(message, 'text/plain')
    html_body: str = get_body(message, 'text/html')

    return {
        'to': get_header(payload.get('headers'), 'To'),
        'subject': subject,
        'body': text_body,
        'html': html_body,
        'attachments': get_attachments(payload.get('parts'), message['id'], service),
        'contact': {
            'name': sender_name,
            'post': None,
            'email': sender_address,
            'phone': None,
            'skype': None,
            'company': {
                'name': None,
                'email': None,
                'address': None,
            },
        },
    }


def get_header(headers: List[Dict[str, str]], header_name: str) -> Optional[str]:
    """Возвращает значение указанного SMTP-заголовка в письме."""
    if not headers:
        return None

    header: Dict[str, str]
    for header in headers:
        if header['name'] == header_name:
            return header['value']
    return None


def get_subject(payload: Dict[str, Any]) -> str:
    """Возвращает тему письма."""
    subject: str = get_header(payload.get('headers'), 'Subject')
    if not subject:
        return ''

    prefix: str
    for prefix in ('Re:', 're:', 'Fwd:', 'fwd:'):
        subject.replace(prefix, '')

    return subject.strip()


def _get_body(body: Dict[str, str]) -> bytes:
    """Раскодирует data у body из base64 в строку байт."""
    try:
        return urlsafe_b64decode(body['data'])
    except BinasciiError:
        log.exception('Не удалось декодировать base64 тела сообщения.')
        return b''
    except KeyError:
        log.info('У body нет data (обычно нерелевантный мусор).')
        pp.pprint(body)
        return b''


def get_body(message: Dict[str, Any], mime_type: str) -> str:
    """Возвращает текст тела письма. В случае HTML тела, сначала получает из него только текст."""
    body: Optional[str] = None
    payload: Dict[str, Any] = message.get('payload')
    if not payload:
        log.warning('Не найден payload в body')
        return ''

    parts: List[Dict[str, Any]] = payload.get('parts')
    if not parts:
        if not payload.get('body'):
            subject: str = get_subject(payload)
            log.error(f"У сообщения: '{subject}' не найдено тело")
            return ''
        header: str = get_header(payload.get('headers'), 'Content-Type')
        if header and header.startswith(mime_type):
            body = _get_body(payload.get('body'))
    else:
        body = get_parts_recursively(parts, mime_type)

    if body:
        return body.decode(encoding="utf-8", errors="ignore")
    return ''


def get_parts_recursively(parts: List[Dict[str, Any]], mime_type: str) -> bytes:
    """
    Рекурсивно проходит по вложенным parts в parts и вытаскивает из них body для заданного mime_type
    body на выходе это байты полученные от раскодирования base64 body в parts.
    """
    body: bytes = b''
    part: Dict[str, Any]
    for part in parts:
        header: str = get_header(part.get('headers'), 'Content-Type')
        if header and header.startswith(mime_type):
            try:
                body += _get_body(part['body'])
            except BinasciiError:
                log.exception('Не удалось декодировать base64 тела сообщения.')
                pp.pprint(f"body {part['body']['data']}")
                body = b''
            except KeyError:
                log.exception('В part не найден body.')
                pp.pprint(part)
                raise RuntimeError('В part не найден body.')

        if 'parts' in part:
            body += get_parts_recursively(part['parts'], mime_type)

    return body


def get_attachments(
        parts: List[Dict[str, Any]],
        message_id: str,
        service: Resource,
        attachments: List[Dict[str, Union[str, bytes]]] = list()
) -> List[Optional[Dict[str, Union[str, bytes]]]]:
    """Возвращает коллекцию файлов-вложений письма для последущего экспорта в Amo CRM"""
    part: Dict[str, Any]
    if not parts:
        return list()

    for part in parts:
        header: str = get_header(part.get('headers'), 'Content-Disposition')
        if not header:
            continue
        if not header.startswith('attachment;'):
            continue

        attachment_id: str = part['body']['attachmentId']
        filename: str = part['filename']

        attachment: Dict[str, Any] = service.users().messages().attachments() \
            .get(userId=USER_ID, messageId=message_id, id=attachment_id).execute()

        try:
            attachment: bytes = urlsafe_b64decode(attachment['data'])
        except BinasciiError:
            log.exception("Не удалось декодировать base64 attachment'а.")

        attachments.append({
            'name': filename,
            'data': attachment,
            'mime': part['mimeType'],
            'size': part['body']['size'],
        })
        if 'parts' in part:
            log.info('При разборе вложений обнаружен вложенный parts.')
            attachments += get_attachments(part['parts'], message_id, attachments)

    return attachments


if __name__ == '__main__':
    main()
