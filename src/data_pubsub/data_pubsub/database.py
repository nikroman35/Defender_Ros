import sqlite3
from .FileReader import *
from .loger import *
import logging
import numpy as np
import time

class DatabaseWorker:

    @staticmethod
    def create_connection():
        Loger.set_type("db")
        conn = None
        try:
            conn = sqlite3.connect('data_database', check_same_thread=False)
        except Error as e:
            logging.error("Cant connect database", e)

        return conn

    @staticmethod
    def create_anal_connection():
        Loger.set_type("db")
        conn = None
        try:
            conn = sqlite3.connect('data_database_anal', check_same_thread=False)
        except Error as e:
            logging.error("Cant connect database", e)

        return conn

    @staticmethod
    def write_data(connect, data: ControllerDataClass, time):

        Loger.set_type("db")

        try:
            cursor = connect.cursor()
            task = (time,
                    data.status,
                    data.pitch,
                    data.roll,
                    data.course,
                    data.w_x,
                    data.w_y,
                    data.w_z,
                    data.a_x,
                    data.a_y,
                    data.a_z,
                    data.gps_speed,
                    data.gps_track_angle,
                    data.gps_satellite_number,
                    data.altitude,
                    data.latitude,
                    data.longitude,
                    data.gps_utc_date,
                    data.utc_time,
                    data.targeting,
                    data.temperature)

            sql = ''' INSERT or IGNORE INTO msg_list(msg_time,status,pitch,roll,course,w_x,w_y,w_z,a_x,a_y,a_z,gps_speed,gps_track_angle,gps_satellite_number,altitude,latitude,longitude,gps_utc_date,utc_time,targeting,temperature)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''


            cursor.execute(sql, task)
            connect.commit()
            cursor.close()

        except sqlite3.Error as error:
            # print("Failed to update1 sqlite table", error)
            logging.error("Failed to update1 sqlite table %s", error)

        finally:
            if connect:
                logging.info("The SQLite connection is closed")

    @staticmethod
    def write_send_mark(connect, time, status):
        Loger.set_type("db")
        try:
            cursor = connect.cursor()
            task = (status, time)

            sql = ''' UPDATE or IGNORE msg_list SET isSent = ? where msg_time = ?'''

            cursor.execute(sql, task)
            connect.commit()
            cursor.close()

        except sqlite3.Error as error:
            print("Failed to update2 state sqlite table", error)
            logging.error("Failed to update2 sqlite table %s", error)
        finally:
            if connect:
                #connect.close()
                return time

    @staticmethod
    def read_unsent_data(connect):
        Loger.set_type("db")
        try:
            cursor = connect.cursor()
            sql = ''' SELECT * from msg_list '''
            cursor.execute(sql)
            records = cursor.fetchall()

            cursor.close()
            #print(len(records), "_____РАЗМЕР SELECT___")
            return records

        except sqlite3.Error as error:
            print("Ошибка при работе с SQLite", error)
            logging.error("Failed to read sqlite table %s", error)
        finally:
            if connect:
                connect.close()
                logging.info("The SQLite connection is closed")





