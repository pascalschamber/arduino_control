import threading
from abc import ABC, abstractmethod
import time
import random
import serial
import os
from pathlib import Path
import logging
from datetime import datetime


def get_current_datetime():
    return time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())


class Clock:
    def __init__(self, duration, time_resolution):
        self.duration = duration
        self.time_resolution = time_resolution
        self.current_time = 0
        self.ended = False

    def tick(self):
        self.start_time = time.time()
        while True:
            self.current_time = time.time() - self.start_time
            if self.current_time >= self.duration:
                self.ended = True
                break
            time.sleep(self.time_resolution)
        self.log("clock duration reached.")
    
    def log(self, msg, msg_type='info'):
        log_func = getattr(self.composer.logger.logger, msg_type)
        log_func(f"[Clock]: {msg}")
       


class Randomizer:
    def __init__(self, signals, repeat_count):
        self.signals = signals
        self.repeat_count = repeat_count

    def shuffle(self):
        # Repeat each element the specified number of times
        order = [el for el in self.signals for _ in range(self.repeat_count)]
        random.shuffle(order)  # Shuffle the list
        return order
    

class MyLogger:
    def __init__(self, log_lvl=logging.INFO, log_output_path=None, print_log=bool(1)):
        # Create a custom logger
        log_path = self.setup_logging(log_output_path)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_lvl)  # Set the log level
        self.logger.handlers = []  # Clear existing handlers

        # create the handler for writing to console
        fmt = "%(asctime)s [%(levelname)s] --> %(message)s"
        datefmt = "%H:%M:%S"
        if print_log:
            c_handler = logging.StreamHandler()
            c_format = CustomFormatter(fmt=fmt, datefmt=datefmt)
            c_handler.setFormatter(c_format)
            self.logger.addHandler(c_handler)
        # create the handler for writing log to disk
        f_handler = logging.FileHandler(log_path)
        f_format = CustomFormatter(fmt=fmt, datefmt=datefmt)
        f_handler.setFormatter(f_format)
        self.logger.addHandler(f_handler)

        # Test logging
        self.logger.info(f"logger setup, writing to {log_path}.")
    

    def setup_logging(self, log_output_path):
        # Determine the log file path
        now = get_current_datetime()

        if log_output_path:
            log_file_path = Path(log_output_path)
        else:
            log_file_path = Path(__file__).parent / "logs" / f"{now}.log"

        # Create directories if they don't exist
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        return log_file_path

class CustomFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # Get the creation time of the log record
        created_time = datetime.fromtimestamp(record.created)

        # If a date format is specified, use it, but append milliseconds
        if datefmt:
            formatted_time = created_time.strftime(datefmt)
            # Add milliseconds, rounded to 2 decimal places
            millis = f"{created_time.microsecond / 1000000:.2f}".split('.')[1]
            return f"{formatted_time}.{millis}"
        else:
            # Default format if no date format is provided
            return super().formatTime(record, datefmt)
        


#############################################################################################
class Composer(threading.Thread):
    def __init__(self, WAIT_FOR_GO=bool(1), config=None):
        super().__init__()

        self.WAIT_FOR_GO = WAIT_FOR_GO

        self.controllers = []
        self.clock = None
        self.randomizer = None
        self.logger = None

        self.event_list = None
        self.event_schedule = {}
        self.event_handled = threading.Event()
        self.running = True
        self.next_event_time = None

        self.keys_to_check = ["clock", "randomizer", "logger", "controllers"]
        self.ingest_config(config)

        # create the event order
        self.create_event_order()
        
    
    def maybe_wait_to_start(self):
        if self.WAIT_FOR_GO:
            while True:
                inp = input("type 'y' to start, 'exit' to end process").lower()
                if inp == 'y':
                    break
                elif inp == 'exit':
                    self.end()

    def main(self):
        try:
            # initialize the controllers
            for controller in self.controllers:
                controller.start()
            self.maybe_wait_to_start()
            self.log('starting main loop')

            # Start the clock, controllers, and composer threads
            clock_thread = threading.Thread(target=self.clock.tick)
            clock_thread.start()
            composer.start()
            composer.join()  # Wait for the composer to finish

        except KeyboardInterrupt:
            pass

        finally:
            # Graceful shutdown
            self.log("Shutting down all threads...")
            clock_thread.join()
            for controller in self.controllers:
                controller.join()
            self.log("Program finished.")
    
    def run(self):
        self.create_event_schedule()
        while not self.clock.ended:
            if (
                self.next_event_time is not None
                and self.clock.current_time >= self.next_event_time
            ):
                # pass the event value to the controllers
                for controller, value in self.event_schedule[self.next_event_time]:
                    controller.event_value = value
                    controller.trigger_event.set()
                # remove the event once it has been triggered
                del self.event_schedule[self.next_event_time]
                self.next_event_time = min(self.event_schedule.keys(), default=None)

            time.sleep(self.clock.time_resolution)
        self.log("event loop completed")

        self.end()
        

    def ingest_config(self, config):
        # check all the neccessary args are provided
        assert all(key in config for key in self.keys_to_check)

        for key in self.keys_to_check:
            if key == "controllers":
                for arg_dict in config[key]:
                    obj_type, obj_args = self.parse_obj_args(arg_dict)
                    self.add_controller(obj_type(**obj_args))
            else:
                obj_type, obj_args = self.parse_obj_args(config[key])
                setattr(self, key, obj_type(**obj_args))
                setattr(getattr(self, key), 'composer', self)

        self.log(f"composer params:\n{self}") # log the params

    def parse_obj_args(self, adict):
        obj_type = adict["type"]
        del adict["type"]
        return obj_type, adict

    def add_controller(self, controller):
        # check num signals matches ouput channels
        assert len(self.randomizer.signals) == len(controller.event_value_map)
        self.controllers.append(controller)
        controller.set_composer(self)

    def create_event_order(self):
        # shuffle the outputs n times and generate a list of ouputs to send commands to and when
        order = self.randomizer.shuffle()
        # get the times events should happen
        command_times = [
            self.clock.duration // len(order) * i for i in range(len(order))
        ]
        assert (
            command_times[-1] < self.clock.duration
        )  # make sure no events happen outside the intended duration
        assert len(order) == len(command_times)

        self.event_list = [(command_times[i], order[i]) for i in range(len(order))]
        event_list_str = "\n\t".join([str(el) for el in self.event_list])
        self.log(f"randomized event order:\n\t{event_list_str}")

    def create_event_schedule(self):
        # tell the threads when they need to wait until
        self.event_schedule = {}
        for controller in self.controllers:
            for event_time, value in self.event_list:
                self.event_schedule.setdefault(event_time, []).append(
                    (controller, value)
                )
        self.next_event_time = min(self.event_schedule.keys())  # init the first event

    
    def end(self):
        for controller in self.controllers:
            controller.shutdown()
        self.log("shutdown complete.")

    def notify_event_handled(self, controller_name):
        self.log(
            f"{controller_name} has completed its event at time {self.clock.current_time}"
        )
        self.event_handled.set()
    
    def log(self, msg, msg_type='info'):
        log_func = getattr(self.logger.logger, msg_type)
        log_func(f"[Composer]: {msg}")
    
    def __str__(self):
        astr = ''
        for attr in dir(self):
            if not attr.startswith('__') and not callable(getattr(self, attr)):
                astr += f"{attr}: {str(getattr(self,attr))}\n"
        return astr


#############################################################################################
class ControllerBase(threading.Thread, ABC):
    def __init__(self, **kwargs):
        threading.Thread.__init__(self)
        self.name = None
        self.port = None
        self.event_value_map = None
        self.startup_commands=[]
        self.event_commands=[]
        self.shutdown_commands=[]
        self.startup_sleep_time=0.1
        self.event_duration = 1.0
        self.event_wait_timeout = 0.1 # how much time to wait inbetween checking if event triggered
        self.shutdown_sleep_time=0.1
        self.baudrate = 9600
        self.timeout = 1.0
        self.board = None
        self.composer = None
        self.trigger_event = threading.Event()
        self.event_value = None
        self.running = True
        self.connected = False

        for k, v in kwargs.items():
            setattr(self, k, v)

    def __str__(self):
        return f"{self.name} ({self.port})"

    def set_composer(self, composer):
        self.composer = composer

    def get_elapsed_time(self, r=2):
        et = self.composer.clock.current_time
        return et if round is None else round(et, r)

    def run(self):
        self.startup()
        while self.running:
            self.trigger_event.wait(timeout=self.event_wait_timeout)  # Add a timeout for periodic checking
            if self.trigger_event.is_set():
                target = self.event_value_map[self.event_value]
                self.log(f"triggered - value:{self.event_value}, target:{target}")
                self.handle_event(target)
                self.trigger_event.clear()
                if self.composer:
                    self.composer.notify_event_handled(self.name)

    def startup(self):
        self.log(f"startup..")
        self.board = serial.Serial(self.port, baudrate=self.baudrate, timeout=self.timeout)
        self.connect()
        self.connected = True
        self.log(f"startup complete.")

    def shutdown(self):
        self.log(f"shutdown..")
        self.running = False
        self.disconnect()
        self.connected = False
        self.log(f"shutdown complete.")

    def log(self, msg, msg_type='info'):
        log_func = getattr(self.composer.logger.logger, msg_type)
        log_func(f"[{self}]: {msg}, (elapsed time:{self.get_elapsed_time()})")

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def handle_event(self, target, value):
        pass


class ArduinoController(ControllerBase):
    def send_command(self, target, value, sleep_time=1.0):
        assert isinstance(value, str) and isinstance(target, int)
        self.board.write(bytes([target, ord(value)]))
        self.log(f"send_command --> target:{target}, value:{value}.")
        time.sleep(sleep_time)

    def connect(self):
        for target in self.event_value_map.values():
            for startup_cmd in self.startup_commands:
                self.send_command(target, startup_cmd, sleep_time=self.startup_sleep_time)

    def handle_event(self, target):
        for cmd in self.event_commands:
            self.send_command(target, cmd, sleep_time=self.event_duration)

    def disconnect(self):
        for target in self.event_value_map.values():
            self.send_command(target, "0", sleep_time=self.shutdown_sleep_time)
        self.board.close()
        
        
class PumpController(ControllerBase):
    def send_command(self, cmd, sleep_time=1.0):
        self.board.write(cmd.encode() + b'\r\n')
        response = self.board.readline().decode().strip()
        self.log(f"send_command --> cmd:{cmd}, response:{response}")
        time.sleep(sleep_time)

    def connect(self):
        for target in self.event_value_map.keys():
            for cmd in [f"L{target}"] + self.startup_commands:
                self.send_command(cmd, sleep_time=self.startup_sleep_time)
        
    def handle_event(self, target):
        self.send_command(f"L{target}", sleep_time=0.1) # make target cmd is quick
        for cmd in self.event_commands:
            self.send_command(cmd, sleep_time=self.event_duration)
            
    def disconnect(self):
        for target in self.event_value_map.keys():
            for cmd in [f"L{target}"] + self.shutdown_commands:
                self.send_command(cmd, sleep_time=self.shutdown_sleep_time)
        self.board.close()


#############################################################################################
# conda activate arduino_control
# python "G:\My Drive\Code\JamieRotation\ValenceTestController.py"

if __name__ == "__main__":
    config = dict(
        clock=dict(type=Clock, duration=30, time_resolution=0.1),
        randomizer=dict(type=Randomizer, signals=["1", "2"], repeat_count=2),
        logger=dict(type=MyLogger, log_lvl=logging.INFO, log_output_path=None, print_log=bool(1)),
        controllers=[
            dict(
                type=ArduinoController,
                name="ArduinoController1",
                port="COM3",
                event_value_map={"1": 3, "2": 5},
                startup_commands=["0"],
                startup_sleep_time=0.1,
                event_commands=["1", "0"],
                event_duration=1.0,
                shutdown_commands=["0"],
                shutdown_sleep_time=0.1,
                baudrate=9600,
                timeout=1,
            ),
            dict(
                type=PumpController,
                name="PumpController1",
                port="COM4",
                event_value_map={"1": "1", "2": "2"},
                startup_commands=['?C', 'N', 'T6', 'S', 'I', 'V5000', 'R1482'],
                startup_sleep_time=0.1,
                event_commands=["G"],
                event_duration=1.0,
                shutdown_commands=['?C'],
                shutdown_sleep_time=0.1,
                baudrate=9600,
                timeout=1,
            ),
        ][0:1],
    )


    # Initialize the composer
    composer = Composer(WAIT_FOR_GO=bool(1), config=config)

    if bool(1):
        composer.main()
        
