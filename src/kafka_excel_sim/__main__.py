import argparse
import logging
import kafka_excel_sim
from pathlib import Path


def setup_logging(verbose: bool = False, debug: bool = False):
    """Configure logging based on verbosity level."""
    level = logging.DEBUG if verbose or debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger('apscheduler').setLevel(logging.DEBUG if debug else logging.WARNING)


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Excel Simulator - Send mock messages to Kafka from an excel file"
    )
    parser.add_argument(
        "config_file",
        type=Path,
        nargs="?",
        default=Path("configuration.xlsx"),
        help="Path to the Excel configuration file (default: configuration.xlsx)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose console output",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug console output",
    )
    args = parser.parse_args()

    setup_logging(args.verbose, args.debug)

    config_file_path = args.config_file

    if not config_file_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_file_path}")

    with kafka_excel_sim.ExcelReader(config_file_path) as reader:
        config = reader.load_config()
        templates = reader.load_types()
        schedule_list = reader.load_schedule()
        reader.validate(templates, schedule_list)
    
    kafka_config: kafka_excel_sim.KafkaConfig = kafka_excel_sim.KafkaConfig.from_dict(config['KAFKA CLIENT'])
    message_config = kafka_excel_sim.MessageConfig.from_dict(config['MESSAGES OPTIONS'])
    builder = kafka_excel_sim.MessageBuilder(templates, message_config)
    kafka_client = kafka_excel_sim.KafkaClient(kafka_config)
    sender = kafka_excel_sim.MessageSender(kafka_client)

    scheduler = kafka_excel_sim.Scheduler(
        schedule = schedule_list,
        builder = builder,
        sender = sender,
    )

    scheduler.run()

if __name__ == "__main__":
    main()