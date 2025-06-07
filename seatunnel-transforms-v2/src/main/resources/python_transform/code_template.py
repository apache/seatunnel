from py4j.java_gateway import JavaGateway, CallbackServerParameters


# 实现 Java 接口的 Python 类 支持关闭资源和运行代码
class PythonCallback:
    def __init__(self, gateway):
        self.gateway = gateway

    def shutdownNow(self):
        print("[Python] 收到Java端关闭通知，Python端开始清理资源...")
        # 在这里添加Python端需要执行的清理工作，比如关闭文件、释放资源等
        import sys
        self.gateway.shutdown();
        sys.exit(0)

    class Java:
        implements = ["org.apache.seatunnel.transform.python"]


def main():
    # 连接到 Java 服务端（默认 25333），并启动 Python 回调服务端（默认 25334）
    gateway = JavaGateway(
        callback_server_parameters=CallbackServerParameters()
    )
    # 获取 Java 对象
    java_app = gateway.entry_point

    # 注册 Python 回调到 Java
    python_callback = PythonCallback(gateway)
    java_app.registerCloseRemotePython(python_callback)
    try:
        ## 执行 java段给定的自定义代码
        exec(java_app.getPythonCode())  # 动态执行 Python 代码
    except Exception as e:
        print(f"[Python] 代码执行出错: {e}")


if __name__ == "__main__":
    main()
