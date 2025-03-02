import rclpy
from rclpy.logging import get_logger
from rclpy.node import Node
from nav_msgs.msg import Odometry
from sensor_msgs.msg import LaserScan
from ackermann_msgs.msg import AckermannDriveStamped

import socket
import threading
import queue
import time
import pickle
import select


class Comms(Node):
    def __init__(self):
        super().__init__("comms")

        self._scans_queue = queue.Queue(maxsize=0)
        self._odoms_queue = queue.Queue(maxsize=0)
        self._actions_queue = queue.Queue(maxsize=0)

        self._setup_subscriptions()
        self._setup_publisher()
        self._setup_socket()

    def _setup_subscriptions(self):
        self._scan_subscription = self.create_subscription(
            LaserScan, "/scan", self._scan_callback, 10
        )
        self._odom_subscription = self.create_subscription(
            Odometry, "/ego_racecar/odom", self._odom_callback, 10
        )

    def _scan_callback(self, scan):
        # self.get_logger().info("scan")
        self._scans_queue.put(scan, block=False)

    def _odom_callback(self, odom):
        # self.get_logger().info("odom")
        self._odoms_queue.put(odom, block=False)

    def _setup_publisher(self):
        self._action_publisher = self.create_publisher(
            AckermannDriveStamped, "/drive", 10
        )
        self.timer = self.create_timer(1.0, self._timer_callback)

    def _empty_actions_queue(self):
        action = None
        while not self._actions_queue.empty():
            action = self._actions_queue.get(block=False)
            self._actions_queue.task_done()

        return action

    def _timer_callback(self):
        action = self._empty_actions_queue()
        if action is None:
            return

        msg = AckermannDriveStamped()
        msg.drive.speed = action.speed
        msg.drive.steering_angle = action.steering_angle

        self._action_publisher.publish(msg)

    def _setup_socket(self):
        self._stop_event = threading.Event()

        self._socket_thread = threading.Thread(
            target=CommsSocket._socket_worker,
            args=(
                self._stop_event,
                self._scans_queue,
                self._odoms_queue,
                self._actions_queue,
            ),
        )
        self._socket_thread.start()

    def cleanup(self):
        self.destroy_subscription(self._scan_subscription)
        self.destroy_subscription(self._odom_subscription)

        self._empty_actions_queue()
        self.destroy_publisher(self._action_publisher)

        self._stop_event.set()

        self._socket_thread.join()
        self._odoms_queue.join()
        self._scans_queue.join()
        self._actions_queue.join()


class CommsSocket:
    logger = get_logger("CommsSocket")

    def _empty_queues(scans_queue, odoms_queue):
        scan = None
        odom = None

        while not scans_queue.empty():
            scan = scans_queue.get(block=False)
            scans_queue.task_done()

        while not odoms_queue.empty():
            odom = odoms_queue.get(block=False)
            odoms_queue.task_done()

        return scan, odom

    def _start_server():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.setblocking(False)
        server.bind(("0.0.0.0", 3000))
        server.listen(1)

        CommsSocket.logger.info("Socket is listenting on port 3000...")

        return server

    def _send_full_message(sock, obj):
        message = pickle.dumps(obj)
        msg_length = len(message)
        sock.sendall(msg_length.to_bytes(4, "big"))
        sock.sendall(message)

    def _receive_full_message(conn):
        ready, _, _ = select.select([conn], [], [], 0)

        if len(ready):
            msg_length_data = conn.recv(4)
            if not msg_length_data:
                return None
            msg_length = int.from_bytes(msg_length_data, "big")
        else:
            return None

        data_chunks = []
        bytes_received = 0
        while bytes_received < msg_length:
            chunk = conn.recv(min(1024, msg_length - bytes_received))
            if not chunk:
                break

            data_chunks.append(chunk)
            bytes_received += len(chunk)

        if bytes_received < msg_length:
            return None

        return pickle.loads(b"".join(data_chunks))

    def _build_obs_dict(scan: LaserScan, odom: Odometry):
        return {
            "scan": {
                "scan_lines": list(scan.ranges),
                "max_range": scan.range_max,
                "min_range": scan.range_min,
                "fov": (scan.angle_max - scan.angle_min),
            },
            "odom": {
                "linear_velocity_x": odom.twist.twist.linear.x,
                "linear_velocity_y": odom.twist.twist.linear.y,
                "angular_velocity_z": odom.twist.twist.angular.z,
            },
        }

    def _extract_action_pickle(data): ...

    def _socket_worker(stop_event, scans_queue, odoms_queue, actions_queue):
        server = CommsSocket._start_server()

        while not stop_event.is_set():
            try:
                conn, addr = server.accept()
                CommsSocket.logger.info(f"Connected by {addr}")

                while True:
                    time.sleep(1)
                    if stop_event.is_set():
                        conn.close()
                        break

                    scan, odom = CommsSocket._empty_queues(scans_queue, odoms_queue)
                    if scan is None or odom is None:
                        continue

                    try:
                        obs_dict = CommsSocket._build_obs_dict(scan, odom)
                        CommsSocket._send_full_message(conn, obs_dict)

                        action_dict = CommsSocket._receive_full_message(conn)
                        if action_dict is None:
                            continue

                        actions_queue.put(action_dict)
                    except (BrokenPipeError, ConnectionResetError):
                        CommsSocket.logger.info("Connection closed by client")
                        conn.close()
                        break
            except BlockingIOError:
                continue

        server.close()
        CommsSocket._empty_queues(scans_queue, odoms_queue)


def main(args=None):
    rclpy.init(args=args)
    node = Comms()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().info("Keyboard Interrupt encountered. Shutting down")
    finally:
        node.cleanup()
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
