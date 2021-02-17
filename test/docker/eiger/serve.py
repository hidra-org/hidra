import http.server
import json
import socketserver
import os
import pathlib
import sys

PORT = 80


def update_index_file(filenames, api_version="1.8.0"):
    if api_version == "1.8.0":
        if not filenames:
            # No files are returned as None
            filenames = None
        content = {"value": filenames, "value_type": "list"}
    elif api_version == "1.6.0":
        content = filenames
    else:
        raise ValueError("Unknown API version: " + api_version)

    index_file = "filewriter/api/{}/files/index.html".format(
        api_version)

    with open(index_file, "w") as f:
        f.write(json.dumps(content))


def list_files():
    parent = "data"
    file_list = [
        path.relative_to(parent).as_posix()
        for path in sorted(pathlib.Path(parent).glob("**/*"))
        if path.is_file()]

    return file_list


def update_index():
    print("Updating index")
    filenames = list_files()
    for api_version in ["1.8.0", "1.6.0"]:
        update_index_file(filenames, api_version=api_version)


class MyHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def send_head(self):
        print("self.path =", self.path)
        if self.path in [
                "/filewriter/api/1.6.0/files/",
                "/filewriter/api/1.8.0/files/"]:
            update_index()
        return super().send_head()

    def do_DELETE(self):
        path = self.translate_path(self.path)
        try:
            os.remove(path)
            self.send_response(http.server.HTTPStatus.NO_CONTENT)
            self.end_headers()
        except FileNotFoundError:
            self.send_error(http.server.HTTPStatus.NOT_FOUND, "File not found")
        except OSError:
            self.send_error(http.server.HTTPStatus.FORBIDDEN, "Not allowed")


Handler = MyHTTPRequestHandler
httpd = socketserver.TCPServer(("", PORT), Handler)

print("serving at port", PORT)
try:
    httpd.serve_forever()
except KeyboardInterrupt:
    print("\nKeyboard interrupt received, exiting.")
    httpd.server_close()
    sys.exit(0)
