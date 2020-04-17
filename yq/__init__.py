"""
yq: Command-line YAML processor - jq wrapper for YAML documents

yq transcodes YAML documents to JSON and passes them to jq.
See https://github.com/kislyuk/yq for more information.
"""

# PYTHON_ARGCOMPLETE_OK

from __future__ import absolute_import, division, print_function, unicode_literals

import sys, argparse, subprocess, json, os, os.path, tempfile
from collections import OrderedDict
from datetime import datetime, date, time

import yaml, argcomplete

from .compat import USING_PYTHON2, open
from .parser import get_parser, jq_arg_spec
from .loader import get_loader
from .dumper import get_dumper
from .version import __version__  # noqa

class JSONDateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime, date, time)):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

def mktempfifo(input_stream, suffix, program_name=sys.argv[0]):
    mktemp_kwargs = {}
    if input_stream == sys.stdin:
        return None
    (dir, basename) = os.path.split(input_stream.name)
    mktemp_kwargs = {
        "dir": dir,
        "prefix": basename + ".tmp_",
        "suffix": suffix
    }
    fifo_name = tempfile.mktemp(**mktemp_kwargs)
    try:
        os.mkfifo(fifo_name)
        return fifo_name
    except FileExistsError as e:
        msg = "{}: Error creating fifo {} for file {}: {}. Skipping."
        sys.stderr.print(msg.format(program_name, fifo_name, input_stream.name, type(e).__name__))
        return None

def stream_input_docs(input_streams_with_targets, input_format):
    for input_stream, target in input_streams_with_targets:
        with open(input_fifo_name, 'w') as input_fifo:
            for input_doc in load_input_docs(input_stream, input_format):
                json.dump(input_doc, input_fifo)

def load_input_docs(input_stream, input_format):
    if input_format == "yaml":
        loader = get_loader(use_annotations=use_annotations)
        for doc in yaml.load_all(input_stream, Loader=loader):
            yield doc
    elif input_format == "xml":
        import xmltodict
        yield xmltodict.parse(input_stream.read(), disable_entities=True)
    elif input_format == "toml":
        import toml
        yield toml.load(input_stream)
    else:
        raise Exception("Unknown input format")

def decode_output_docs(jq_output, json_decoder):
    while jq_output:
        doc, pos = json_decoder.raw_decode(jq_output)
        jq_output = jq_output[pos + 1:]
        yield doc

def xq_cli():
    cli(input_format="xml", program_name="xq")

def tq_cli():
    cli(input_format="toml", program_name="tq")

class DeferredOutputStream:
    def __init__(self, name, mode="w"):
        self.name = name
        self.mode = mode
        self._fh = None

    @property
    def fh(self):
        if self._fh is None:
            self._fh = open(self.name, self.mode)
        return self._fh

    def flush(self):
        if self._fh is not None:
            return self.fh.flush()

    def close(self):
        if self._fh is not None:
            return self.fh.close()

    def __getattr__(self, a):
        return getattr(self.fh, a)

class LazyFifodInputStream:
    def __init__(self, input_stream):
        self.input_stream = input_stream


def cli(args=None, input_format="yaml", program_name="yq"):
    parser = get_parser(program_name, __doc__)
    argcomplete.autocomplete(parser)
    args, jq_args = parser.parse_known_args(args=args)

    for i, arg in enumerate(jq_args):
        if arg.startswith("-") and not arg.startswith("--"):
            if "i" in arg:
                args.in_place = True
            if "y" in arg:
                args.output_format = "yaml"
            elif "Y" in arg:
                args.output_format = "annotated_yaml"
            elif "x" in arg:
                args.output_format = "xml"
            jq_args[i] = arg.replace("i", "").replace("x", "").replace("y", "").replace("Y", "")
        if args.output_format != "json":
            jq_args[i] = jq_args[i].replace("C", "")
            if jq_args[i] == "-":
                jq_args[i] = None

    jq_args = [arg for arg in jq_args if arg is not None]

    for arg in jq_arg_spec:
        values = getattr(args, arg, None)
        delattr(args, arg)
        if values is not None:
            for value_group in values:
                jq_args.append(arg)
                jq_args.extend(value_group)

    if "--from-file" in jq_args or "-f" in jq_args:
        args.input_streams.insert(0, argparse.FileType()(args.jq_filter))
    else:
        jq_filter_arg_loc = len(jq_args)
        if "--args" in jq_args:
            jq_filter_arg_loc = jq_args.index('--args') + 1
        elif "--jsonargs" in jq_args:
            jq_filter_arg_loc = jq_args.index('--jsonargs') + 1
        jq_args.insert(jq_filter_arg_loc, args.jq_filter)
    delattr(args, "jq_filter")
    in_place = args.in_place
    delattr(args, "in_place")

    if sys.stdin.isatty() and not args.input_streams:
        return parser.print_help()

    yq_args = dict(input_format=input_format, program_name=program_name, jq_args=jq_args, **vars(args))
    if in_place:
        if USING_PYTHON2:
            sys.exit("{}: -i/--in-place is not compatible with Python 2".format(program_name))
        if args.output_format not in {"yaml", "annotated_yaml"}:
            sys.exit("{}: -i/--in-place can only be used with -y/-Y".format(program_name))
        input_streams = yq_args.pop("input_streams")
        if len(input_streams) == 1 and input_streams[0].name == "<stdin>":
            msg = "{}: -i/--in-place can only be used with filename arguments, not on standard input"
            sys.exit(msg.format(program_name))
        for i, input_stream in enumerate(input_streams):
            def exit_handler(arg=None):
                if arg:
                    sys.exit(arg)
            if i < len(input_streams):
                yq_args["exit_func"] = exit_handler
            yq(input_streams=[input_stream], output_stream=DeferredOutputStream(input_stream.name), **yq_args)
    else:
        yq(**yq_args)

def yq(input_streams=None, output_stream=None, input_format="yaml", output_format="json",
       program_name="yq", width=None, indentless_lists=False, xml_root=None, xml_dtd=False,
       jq_args=frozenset(), exit_func=None):
    if not input_streams:
        input_streams = [sys.stdin]
    if not output_stream:
        output_stream = sys.stdout
    if not exit_func:
        exit_func = sys.exit
    converting_output = True if output_format != "json" else False

    def lazy_fifo(jq_stdin)
    input_streams_with_lazy_fifos = [
        (input_stream, mktempfifo(input_stream, ".json", program_name))
        for input_stream
        in input_streams
    ]

    try:
        # Note: universal_newlines is just a way to induce subprocess to make stdin a text buffer and encode it for us
        jq = subprocess.Popen(["jq"] + list(jq_args) + [fifo_name for _, fifo_name in input_streams_with_fifo_names],
                              stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE if converting_output else None,
                              universal_newlines=True)
    except OSError as e:
        msg = "{}: Error starting jq: {}: {}. Is jq installed and available on PATH?"
        exit_func(msg.format(program_name, type(e).__name__, e))

    try:
        stream_input_docs(input_streams_with_lazy_fifos, input_format)

        if converting_output
            decode_output_stream(

        if converting_output:
            # TODO: enable true streaming in this branch (with asyncio, asyncproc, a multi-shot variant of
            # subprocess.Popen._communicate, etc.)
            # See https://stackoverflow.com/questions/375427/non-blocking-read-on-a-subprocess-pipe-in-python
            use_annotations = True if output_format == "annotated_yaml" else False



            json_decoder = json.JSONDecoder(object_pairs_hook=OrderedDict)
            if output_format == "yaml" or output_format == "annotated_yaml":
                yaml.dump_all(decode_output_docs(jq_out, json_decoder), stream=output_stream,
                              Dumper=get_dumper(use_annotations=use_annotations, indentless=indentless_lists),
                              width=width, allow_unicode=True, default_flow_style=False)
            elif output_format == "xml":
                import xmltodict
                for doc in decode_output_docs(jq_out, json_decoder):
                    if xml_root:
                        doc = {xml_root: doc}
                    elif not isinstance(doc, OrderedDict):
                        msg = ("{}: Error converting JSON to XML: cannot represent non-object types at top level. "
                               "Use --xml-root=name to envelope your output with a root element.")
                        exit_func(msg.format(program_name))
                    full_document = True if xml_dtd else False
                    try:
                        xmltodict.unparse(doc, output=output_stream, full_document=full_document, pretty=True,
                                          indent="  ")
                    except ValueError as e:
                        if "Document must have exactly one root" in str(e):
                            raise Exception(str(e) + " Use --xml-root=name to envelope your output with a root element")
                        else:
                            raise
                    output_stream.write(b"\n" if sys.version_info < (3, 0) else "\n")
            elif output_format == "toml":
                import toml
                for doc in decode_output_docs(jq_out, json_decoder):
                    if not isinstance(doc, OrderedDict):
                        msg = "{}: Error converting JSON to TOML: cannot represent non-object types at top level."
                        exit_func(msg.format(program_name))

                    if USING_PYTHON2:
                        # For Python 2, dump the string and encode it into bytes.
                        output = toml.dumps(doc)
                        output_stream.write(output.encode("utf-8"))
                    else:
                        # For Python 3, write the unicode to the buffer directly.
                        toml.dump(doc, output_stream)
        else:

            jq.wait()
        for input_stream in input_streams:
            input_stream.close()
        exit_func(jq.returncode)
    except Exception as e:
        exit_func("{}: Error running jq: {}: {}.".format(program_name, type(e).__name__, e))
