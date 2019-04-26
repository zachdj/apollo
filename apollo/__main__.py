'''The main Apollo entrypoint.

This is a toolbox API. It is invoked as the `apollo` command followed by a
subcommand. Each subcommand is defined as a module in `apollo.bin`.
'''

import argparse
import importlib
import logging
import pkgutil
import sys


def subcommands(_pkg_name='apollo.bin', _prefix=''):
    '''Iterate over the names of all subcommands.

    Yields:
        str:
            Names of submodules of ``apollo.bin``.
    '''
    # The containing package must be imported to list submodules.
    pkg = importlib.import_module(_pkg_name)
    path = pkg.__path__

    # For all submodules...
    for mod_info in pkgutil.iter_modules(path, _prefix):
        # Skip private modules/commands.
        if mod_info.name.startswith('_'):
            continue

        # If the submodule is a package, recurse.
        elif mod_info.ispkg:
            subpkg_name = f'{_pkg_name}.{mod_info.name}'
            subpkg_prefix = f'{_prefix}{mod_info.name}.'
            yield from subcommands(subpkg_name, subpkg_prefix)

        # Otherwise the submodule is a command.
        else:
            yield mod_info.name


def exec_subcommand(name, argv):
    '''Execute a subcommand with the given name.

    Arguments:
        name (str):
            A dotted module name resolved relative to ``apollo.bin``.
            For example, the string ``"foo.bar"`` refers to the module
            ``apollo.bin.foo.bar``.
        argv (Sequence[str]):
            Additional arguments to pass to the subcommand.

    Raises:
        ValueError:
            A value error is raised if the name does not refer to a valid
            subcommand. Subcommands can be listed by :func:`subcommands`.
    '''
    if name not in subcommands():
        raise ValueError(f'unknown command: {name}')

    # Import the command to access its ``main`` function.
    full_name = f'apollo.bin.{name}'
    mod = importlib.import_module(full_name)

    # We update ``sys.argv[0]`` to fix the help text of the subcommand.
    sys.argv[0] = f'{sys.argv[0]} {name}'

    # Do it.
    mod.main(argv)


def description():
    '''The description of this program for the help text.
    '''
    text = 'The Apollo irradiance forecast system.\n'
    text += '\n'
    text += 'subcommands:\n'
    for cmd in subcommands():
        text += f'  {cmd}\n'
    return text


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=description(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )

    parser.add_argument(
        '-l',
        '--log',
        metavar='LEVEL',
        type=str,
        default='INFO',
        help='the log level (default: INFO)'
    )

    parser.add_argument(
        'command',
        metavar='COMMAND',
        help='the subcommand to execute',
    )

    parser.add_argument(
        'argv',
        metavar='...',  # Matches the usage string generated by argparse.
        nargs=argparse.REMAINDER,
        help='all additional arguments are forwarded to the subcommand'
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        format='[{asctime}] {levelname}: {message}',
        style='{',
        level=args.log
    )

    exec_subcommand(args.command, args.argv)


if __name__ == '__main__':
    main()
