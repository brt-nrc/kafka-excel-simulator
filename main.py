import argparse
import kafka_excel_sim
from pathlib import Path


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
    args = parser.parse_args()

    config_file_path = args.config_file

    if not config_file_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_file_path}")

    with kafka_excel_sim.ExcelReader(config_file_path) as reader:
        config = reader.load_config()
        templates = reader.load_types()
        schedule_list = reader.load_schedule()
        reader.validate(templates, schedule_list)
    
    kafka_config = kafka_excel_sim.KafkaConfig.from_dict(config['KAFKA CLIENT'])
    builder = kafka_excel_sim.MessageBuilder(templates)
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