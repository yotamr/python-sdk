**Overview**
---------
The 'Python SDK' module (hereafter: **The PySDK**) provides a lightweight, efficient, reliable input point to Alooma. The module provides a library of methods to record events and report them to Alooma from any Python application.

**Design**
----------
The module contains two main classes, the PythonSDK and the Sender.

- *PythonSDK*
  - Enqueues events to be reported to the Alooma pipeline. Events are queued into a concurrent queue, from which they are later pulled by the Sender module to be sent to your dedicated Alooma server.

- *Sender*
  - Dequeues messages and sends them to your dedicated Alooma server.
  - Runs as a daemon in the background.

When initialized, the PySDK instantiates a Sender. The Sender starts a separate thread on which a TCP connection is opened. Any disconnection, send error, or other event does not interfere with other code nor crash the service. The module handles errors automatically. Upon disconnection, the module attempts to renew the socket and connection.

**Quick Integration**
-----------
**To integrate the PySDK into your code, please follow these few simple steps:**

1. Run `pip install alooma_pysdk` to install the SDK. Alternatively, the source can be downloaded directly from our [GitHub repository](https://github.com/Aloomaio/python-sdk).

2. Import the module by adding the following line in your Python file:

  ``` Python
  import alooma_pysdk
  ```
3. Initialize a PySDK instance by adding the following line in your Python file:

    ``` Python
sdk = alooma_pysdk.PythonSDK(servers='<your-alooma-server>')
    ```
  - `token` - A arbitrary unique identifier for this input.
  - `servers` - A list of the IPs / Hostnames of your dedicated Alooma servers, supplied by Alooma. To get a server definition, please contact [support@alooma.com](mailto:support@alooma.com)
  - There are many more useful parameters that can be supplied to this function. Here are some key ones (refer to the in-file merthod documentation for more):
      - `port` - The remote port to connect to. Can be left blank, unless specifically instructed otherwise by Alooma support
      - `input_label` - This will give the PySDK a distinct name in the system, which can be used to identify events that were received from this specific SDK integration. This appears as the name of the input node in your Alooma plumbing screen, and defaults to "Python SDK"
      -  `event_type` - A string or callable which receives each event and returns a string. The event type for each event is placed in a `_metadata.event_type` field, which determines the type of the event in the Alooma Mapper screen
      - `callback_func` - A callback function can be provided to be called whenever a log message is emitted from the PySDK
4. To report an event to Alooma simply add this one-liner to your code:

   ``` Python
    sdk.report(event_dict)
    ```
    Alternatively, to report multiple events in one function call, use:

    ``` Python
    sdk.report_many([event_dict, event_dict, ...])
    ```

  In both cases, each event must be a valid Python dictionary or a string. If not, it will be discarded and the callback function, if provided, will be called with an appropriate message.

You are now ready to report events to your Alooma pipeline!
