import luigi
import argparse

from dbupload import DBUpload


def main():
    """
    Application entrypoint.
    - parsing cmd arguments using argparse
    - running luigi tasks
    """

    parser = argparse.ArgumentParser(description='Workflow exercise')
    parser.add_argument('urls', type=str, nargs='+',
                        help='urls to search for')

    args = parser.parse_args()

    luigi.build(
        [DBUpload(urls=args.urls)],
        workers=len(args.urls),
        local_scheduler=False
    )


if __name__ == "__main__":
    main()