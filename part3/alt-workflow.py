#!/usr/bin/env python
# coding: utf-8

import pickle
from pathlib import Path

import pandas as pd
import xgboost as xgb

from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import root_mean_squared_error

from dagster import job, op, Out, Definitions

import mlflow

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("nyc-taxi-experiment")

models_folder = Path('models')
models_folder.mkdir(exist_ok=True)


@op
def get_year():
    return 2023


@op
def get_month():
    return 3


@op(out=Out(pd.DataFrame))
def read_dataframe(year: int, month: int):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    df = pd.read_parquet(url)

    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)

    df['PU_DO'] = df['PULocationID'] + '_' + df['DOLocationID']

    return df


@op(out={"X": Out(object), "dv": Out(object)})
def create_X(df: pd.DataFrame, dv=None):
    categorical = ['PU_DO']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')

    if dv is None:
        dv = DictVectorizer(sparse=True)
        X = dv.fit_transform(dicts)
    else:
        X = dv.transform(dicts)

    return (X, dv)


@op(out={"year": Out(int), "month": Out(int)})
def get_next_month(year: int, month: int):
    next_year = year if month < 12 else year + 1
    next_month = month + 1 if month < 12 else 1
    return (next_year, next_month)


@op(out=Out(str))
def train_model(X_train, y_train, X_val, y_val, dv):
    with mlflow.start_run() as run:
        train = xgb.DMatrix(X_train, label=y_train)
        valid = xgb.DMatrix(X_val, label=y_val)

        best_params = {
            'learning_rate': 0.09585355369315604,
            'max_depth': 30,
            'min_child_weight': 1.060597050922164,
            'objective': 'reg:linear',
            'reg_alpha': 0.018060244040060163,
            'reg_lambda': 0.011658731377413597,
            'seed': 42
        }

        mlflow.log_params(best_params)

        booster = xgb.train(
            params=best_params,
            dtrain=train,
            num_boost_round=30,
            evals=[(valid, 'validation')],
            early_stopping_rounds=50
        )

        y_pred = booster.predict(valid)
        rmse = root_mean_squared_error(y_val, y_pred)
        mlflow.log_metric("rmse", rmse)

        with open("models/preprocessor.b", "wb") as f_out:
            pickle.dump(dv, f_out)
        mlflow.log_artifact("models/preprocessor.b", artifact_path="preprocessor")

        mlflow.xgboost.log_model(booster, artifact_path="models_mlflow")

        return run.info.run_id

@op
def get_target() -> str:
    return "duration"

@op
def extract_target(df: pd.DataFrame, target: str):
    return df[target].values

@job
def run():
    year = get_year()
    month = get_month()

    df_train = read_dataframe(year, month)
    next_month_output = get_next_month(year, month)
    df_val = read_dataframe(next_month_output.year, next_month_output.month)

    train_out = create_X.alias("create_train_x")(df=df_train)
    val_out = create_X.alias("create_val_x")(df=df_val, dv=train_out.dv)
    target = get_target()
    y_train = extract_target.alias("extract_y_train")(df=df_train, target=target)
    y_val = extract_target.alias("extract_y_val")(df=df_val, target=target)

    train_model(X_train=train_out.X, y_train=y_train, X_val=val_out.X, y_val=y_val, dv=train_out.dv)

defs = Definitions(
    jobs=[run],
)