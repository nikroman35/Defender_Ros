import sys
import rclpy
import threading
import functools
import configparser
from datetime import date
import copy
import logging
import queue
import uuid
from rclpy.node import Node
from collections import deque
from interfaces.msg import SpoofingControl
from interfaces.msg import Ins
from interfaces.msg import AnalyzerCallback
from interfaces.msg import AntiSpoofing
from .FileReader import *
from .simulator import *
from .database import *
from .clientTest import *

class FakeDataPublisher(Node):

    def __init__(self, name):
        super().__init__(name)
        self.publisher_ = self.create_publisher(Ins, 'fake_ins_data', 10)

    def callback(self, data):
        msg = Ins()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.status = data.status
        msg.pitch = data.pitch
        msg.roll = data.roll
        msg.course = data.course
        msg.w_x = data.w_x
        msg.w_y = data.w_y
        msg.w_z = data.w_z
        msg.a_x = data.a_x
        msg.a_y = data.a_y
        msg.a_z = data.a_z
        msg.gps_speed = data.gps_speed
        msg.gps_track_angle = data.gps_track_angle
        msg.gps_satellite_number = int(data.gps_satellite_number)
        msg.altitude = data.altitude
        msg.latitude = data.latitude
        msg.longitude = data.longitude
        msg.gps_utc_date = data.gps_utc_date
        msg.utc_time = data.utc_time
        msg.targeting = data.targeting
        msg.temperature = data.temperature

        self.publisher_.publish(msg)


class SimulatorAntiSpoofingPublisher(Node):

    on_state = bytes('1', 'utf-8')
    off_state = bytes('0', 'utf-8')

    def __init__(self):
        super().__init__('simulator_anti_spoofing_class')
        self.publisher_ = self.create_publisher(AntiSpoofing, 'anti_spoofing', 10)

    def callback(self, anal_result, beFake):
        msg = AntiSpoofing()
        if anal_result:
            msg.nav_state = self.on_state
        else:
            msg.nav_state = self.off_state
        if beFake:
            msg.module_state = self.off_state
        else:
            msg.module_state = self.on_state

        self.publisher_.publish(msg)

class SimulatorCallbackPublisher(Node):
    def __init__(self):
        super().__init__('simulator_publisher_class')
        self.publisher_ = self.create_publisher(AnalyzerCallback, 'simulator_topic', 10)

    def callback(self, data):
        msg = AnalyzerCallback()
        msg.check_result = data.check_result
        msg.error_code = data.error_code
        msg.error_description = data.error_description
        self.publisher_.publish(msg)

class SpoofingControlSubscriber(Node):

    on_state = bytes('1', 'utf-8')
    off_state = bytes('0', 'utf-8')

    def __init__(self, topic_name, control_state):
        super().__init__(topic_name)
        callback_lambda = lambda x: self.listener_callback(x, control_state)
        self.subscription = self.create_subscription(
            SpoofingControl,
            topic_name,
            callback_lambda,
            10)
        self.subscription

    def listener_callback(self, msg, control_state):
        if msg.spoof_cntrl_state.decode('utf-8') == '1':
            control_state[0] = self.on_state
            self.get_logger().info('Send ON')
        else:
            control_state[0] = self.off_state
            self.get_logger().info('Send OFF')

class DataSubscriber(Node):

    def __init__(self, topic_name, control_state, q):
        super().__init__("data_node")
        self.database_conn = DatabaseWorker.create_connection()
        self.database_anal_conn = DatabaseWorker.create_anal_connection()
        self.data_buffer = self.get_simulate_data_size()
        self.simulator = Simulator()
        callback_lambda = lambda x: self.listener_callback(x, control_state, q)
        self.subscription = self.create_subscription(
            Ins,
            topic_name,
            callback_lambda,
            10)
        self.subscription

    def listener_callback(self, msg, control_state, q):

        data = self.parse_msg_to_data(msg)
        self.save_data_buffer(data)

        timestamp = msg.header.stamp.sec + (msg.header.stamp.nanosec / 1000000000)
        round_value = round(timestamp, 5)

        logging.info('Real DATA "%f" '
                     ' pitch "%f" '
                     ' roll "%f" '
                     ' course "%f" '
                     ' w_x "%f"'
                     ' w_y "%f"'
                     ' w_z "%f"'
                     ' a_x "%f"'
                     ' a_y "%f"'
                     ' a_z "%f"'
                     ' gps_speed "%f"'
                     ' gps_track_angle "%f"'
                     ' gps_satellite_number "%i"'
                     ' altitude "%f"'
                     ' latitude "%f"'
                     ' longitude "%f"'
                     ' gps_utc_date "%f"'
                     ' utc_time "%f"'
                     ' targeting "%i"'
                     ' temperature "%i"' %
                     (round_value,
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
                      data.temperature))

        threading.Thread(target=self.write_database_var, args=(data, round_value,)).start()

        if control_state[0].decode('utf-8') == '1':
            logging.info('catch attack state time = %s', str(timestamp))
            simulate_value = self.simulate()
            logging.info('SIMULATE DATA "%f" '
                         ' pitch "%f" '
                         ' roll "%f" '
                         ' course "%f" '
                         ' w_x "%f"'
                         ' w_y "%f"'
                         ' w_z "%f"'
                         ' a_x "%f"'
                         ' a_y "%f"'
                         ' a_z "%f"'
                         ' gps_speed "%f"'
                         ' gps_track_angle "%f"'
                         ' gps_satellite_number "%i"'
                         ' altitude "%f"'
                         ' latitude "%f"'
                         ' longitude "%f"'
                         ' gps_utc_date "%f"'
                         ' utc_time "%f"'
                         ' targeting "%i"'
                         ' temperature "%i"' %
                         (round_value,
                          simulate_value.pitch,
                          simulate_value.roll,
                          simulate_value.course,
                          simulate_value.w_x,
                          simulate_value.w_y,
                          simulate_value.w_z,
                          simulate_value.a_x,
                          simulate_value.a_y,
                          simulate_value.a_z,
                          simulate_value.gps_speed,
                          simulate_value.gps_track_angle,
                          simulate_value.gps_satellite_number,
                          simulate_value.altitude,
                          simulate_value.latitude,
                          simulate_value.longitude,
                          simulate_value.gps_utc_date,
                          simulate_value.utc_time,
                          simulate_value.targeting,
                          simulate_value.temperature))
            threading.Thread(target=self.write_simulate_var, args=(simulate_value, round_value,)).start()
        else:
            threading.Thread(target=self.write_simulate_var, args=(data, round_value,)).start()

    def write_database_var(self, data, _time):
        DatabaseWorker.write_data(self.database_conn, data, _time)

    def write_simulate_var(self, data, _time):
        DatabaseWorker.write_data(self.database_anal_conn, data, _time)

    def simulate(self):
        logging.info('simulate data')
        new_value = self.simulator.simulate_new_value(self.data_buffer)
        name = str(uuid.uuid1())
        a = name.replace("-","")
        b = "node" + a
        fake_publisher = FakeDataPublisher(name=b)
        threading.Thread(target=fake_publisher.callback, args=(new_value,)).start()
        return new_value

    def parse_msg_to_data(self, msg: Ins):
        data = ControllerDataClass()
        data.status = msg.status
        data.pitch = msg.pitch
        data.roll = msg.roll
        data.course = msg.course
        data.w_x = msg.w_x
        data.w_y = msg.w_y
        data.w_z = msg.w_z
        data.a_x = msg.a_x
        data.a_y = msg.a_y
        data.a_z = msg.a_z
        data.gps_speed = msg.gps_speed
        data.gps_track_angle = msg.gps_track_angle
        data.gps_satellite_number = msg.gps_satellite_number
        data.altitude = msg.altitude
        data.latitude = msg.latitude
        data.longitude = msg.longitude
        data.gps_utc_date = msg.gps_utc_date
        data.utc_time = msg.utc_time
        data.targeting = msg.targeting
        data.temperature = msg.temperature

        return data

    def save_data_buffer(self, data):
        self.data_buffer.appendleft(data)
        self.data_buffer.pop()

    def get_simulate_data_size(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        queue_size = int(config['SimulatorConfig']['queue_size'])
        d = deque([None] * queue_size)
        return d


simulator_publisher: SimulatorCallbackPublisher
antispoofing_publisher: SimulatorAntiSpoofingPublisher
data_subscriber: DataSubscriber

def main(args=None):
    rclpy.init(args=args)

    Loger.set_type("sim")
    q = deque()

    spoof_control_state = [bytes('0', 'utf-8')]

    global simulator_publisher
    global antispoofing_publisher
    global data_subscriber

    simulator_publisher = SimulatorCallbackPublisher()
    antispoofing_publisher = SimulatorAntiSpoofingPublisher()
    data_subscriber = DataSubscriber('/rtk_1/ins_data', spoof_control_state, q)
    spoofing_subscriber = SpoofingControlSubscriber('spoofing_control', spoof_control_state)

    executor = rclpy.executors.MultiThreadedExecutor()
    executor.add_node(data_subscriber)
    executor.add_node(spoofing_subscriber)
    executor.add_node(simulator_publisher)
    executor.add_node(antispoofing_publisher)

    executor_thread = threading.Thread(target=executor.spin, daemon=True)
    executor_thread.start()
    rate = data_subscriber.create_rate(1)

    try:
        while rclpy.ok():
            rate.sleep()
    except KeyboardInterrupt:
        pass
    rclpy.shutdown()
    executor_thread.join()


if __name__ == '__main__':
    main()
