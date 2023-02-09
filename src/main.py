from src.df_models import InputDataModel, OutputDataModel


def run() -> None:

    # 1. load initial data
    data = InputDataModel().load()

    # 2. process data
    data["engagement_doubled"] = data["engagement"] * 2

    # 3. save data
    OutputDataModel().save(df=data)


if __name__ == '__main__':
    run()
