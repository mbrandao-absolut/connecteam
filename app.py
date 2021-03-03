import json
import logging
import psycopg2
from datetime import datetime
from time import sleep

import tornado.ioloop
import tornado.web
from pytz import timezone
from tornado import escape

class App(tornado.web.Application):
    def __init__(self, *args, **kwargs):
        super(App, self).__init__(*args, **kwargs)


class Handler(tornado.web.RequestHandler):
    def post(self):
        data = json.loads(self.request.body)
        self.process_data(data)

    def data_received(self, chunk: bytes):
        pass

    def process_data(self, data):
        entry = {
            'workflow_entry_id': data['workflowEntryId'],
            'workflow_id': data['workflowId'],
            'entry_number': data['entryNum'],
            'user_name': f"{data['user']['firstName']} {data['user']['lastName']}",
            'user_id': data['user']['userId'],
            'user_email': data['user']['email'],
        }
        date_submitted = datetime.fromisoformat(data['dateSubmitted'])
        entry['date_submitted_utc'] = date_submitted
        entry['date_submitted'] = date_submitted.astimezone(timezone('America/Bahia'))
        for answer in data['workflowEntry']:
            self.add_entry(answer, entry)
        self.load_to_database(entry)
        print(json.dumps(entry, indent=4, default=str))

    def add_entry(self, answer, entry):
        questions = {
            '718de71c-da4a-8c84-7758-61c35334d26a': 'building',
            '408136e0-0307-2ea2-8343-cd64eb400e42': 'room',
            '1b599eda-dd22-4b05-65f3-5fc41a22d2c3': 'date_started_utc',
            'daaf19d0-c005-6733-7616-0e4208cba12b': 'doors_windows_clean',
            '49e3a258-7fee-6672-8c3f-041bf5cffc49': 'walls_clean',
            '12315041-4b8a-e36d-e5cf-347c4ac7dd65': 'floor_clean',
            'c9bb6770-fef4-7e16-f8b2-a07aa3cfa2b7': 'shaders_clean',
            '5605be8b-c4ec-6175-960d-8e7bd8f4ae3a': 'wall_sockets_clean',
            '749936f0-b5a2-53bf-0945-325c3b9f0d24': 'wall_sockets_working',
            'f5933e72-b6e1-f53c-c046-1b7dc4e627fe': 'table_clean',
            '39de403f-dc53-3c09-ca60-5a4875918e20': 'trash_bin_clean',
            '5518bed7-f18a-4c60-c28c-60a691bdd0b8': 'air_conditioner_clean',
            'c61040eb-b054-fe80-3656-41d00f4a9a33': 'air_conditioner_working',
            '744a4b61-8d02-d9b8-293f-013193303d9a': 'room_temperature_right',
            'f472d535-b073-a929-fcc1-034f30ff58a3': 'lights_working',
            'c3f9d19f-85f4-02e4-7c94-fda89c9b5e94': 'chairs_clean',
            'f8e4cf33-85b3-4f43-3c62-e7494bed3e7f': 'enough_chairs',
            '6806ddd8-88cb-e0a6-80c2-897ddd0f3ecd': 'equipment_clean',
            '12d15126-1d48-2059-9dbf-00450baef798': 'equipment_working',
            'c6e3ae75-480c-7aaf-1e54-60b6a88eaf24': 'battery_charged',
            '8ef054ee-c067-d02d-02ce-658542482e7c': 'campaign_ads_on_wall',
            '71ba3fac-5700-33de-b3c2-8c923e74ab42': 'room_plaque_installed',
            'c8a621f5-86d3-caac-fdbe-9df812e42866': 'support_plaques_installed',
            '9c37be9e-332a-f30b-662c-84f46ac38fff': 'problems_description',
        }
        if answer['questionId'] in questions:
            key = questions[answer['questionId']]
            value = self.get_answer(answer)
            entry[key] = value
            if answer['type'] == 'datetime':
                entry[key[:-4]] = value.astimezone(timezone('America/Bahia'))
    
    def get_answer(self, answer):
        if answer['type'] == 'yesNo':
            if len(answer['markedAnswers']) > 0:
                if answer['markedAnswers'][0]['value'] == 'Yes':
                    return True
                else:
                    return False
        elif answer['type'] == 'multipleChoice':
            if len(answer['markedAnswers']) > 0:
                return(answer['markedAnswers'][0]['value'])
        elif answer['type'] == 'openEnded':
            if 'text' in answer:
                return answer['text']
        elif answer['type'] == 'datetime':
            return datetime.fromisoformat(answer['datetime'])
        return None

    def load_to_database(self, entry):
        host = "report-dataset.postgres.database.azure.com"
        dbname = "postgres"
        user = "absolut@report-dataset"
        password = "StkO9YzA6ibH"
        sslmode = "require"

        conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
        conn = psycopg2.connect(conn_string)
        print("Connection established")

        cursor = conn.cursor()

        query = query = """INSERT INTO vistorias
                                (workflow_entry_id,
                                workflow_id,
                                entry_number,
                                user_name,
                                user_id,
                                user_email,
                                date_started_utc,
                                date_started,
                                date_submitted_utc,
                                date_submitted,
                                duration,
                                building,
                                room,
                                doors_windows_clean,
                                walls_clean,
                                floor_clean,
                                shaders_clean,
                                wall_sockets_clean,
                                wall_sockets_working,
                                table_clean,
                                trash_bin_clean,
                                air_conditioner_clean,
                                air_conditioner_working,
                                room_temperature_right,
                                lights_working,
                                chairs_clean,
                                enough_chairs,
                                equipment_clean,
                                equipment_working,
                                battery_charged,
                                campaign_ads_on_wall,
                                room_plaque_installed,
                                support_plaques_installed,
                                problems_description) VALUES
                                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
        print(self.get_values(entry))
        cursor.execute(query, self.get_values(entry))
        print("Inserted row of data")
        conn.commit()
        cursor.close()
        conn.close()
        print(entry.items())

    def get_values(self, entry):
        return (
            entry['workflow_entry_id'],
            entry['workflow_id'],
            entry['entry_number'],
            entry['user_name'],
            entry['user_id'],
            entry['user_email'],
            entry['date_started_utc'],
            entry['date_started'],
            entry['date_submitted_utc'],
            entry['date_submitted'],
            (entry['date_submitted_utc']-entry['date_started_utc']).total_seconds()/60.0,
            entry['building'],
            entry['room'],
            entry['doors_windows_clean'],
            entry['walls_clean'],
            entry['floor_clean'],
            entry['shaders_clean'],
            entry['wall_sockets_clean'],
            entry['wall_sockets_working'],
            entry['table_clean'],
            entry['trash_bin_clean'],
            entry['air_conditioner_clean'],
            entry['air_conditioner_working'],
            entry['room_temperature_right'],
            entry['lights_working'],
            entry['chairs_clean'],
            entry['enough_chairs'],
            entry['equipment_clean'],
            entry['equipment_working'],
            entry['battery_charged'],
            entry['campaign_ads_on_wall'],
            entry['room_plaque_installed'],
            entry['support_plaques_installed'],
            entry['problems_description']
        )

class ErrorHandler(tornado.web.RequestHandler):
    def post(self):
        self.clear()
        self.set_status(404)

    def data_received(self, chunk: bytes):
        pass


if __name__ == '__main__':
    key = '0928a801-efe8-4815-9e22-4c4134c08a19'
    app = App([
        (f'/{key}', Handler),
        ('/.*', ErrorHandler)
    ], debug=True)
    # tornado.ioloop.IOLoop.current().add_callback(processor.watch_queue)
    # tornado.ioloop.IOLoop.current().spawn_callback(watch_queue)
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()
