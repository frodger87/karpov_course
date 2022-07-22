import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context



def check_anomaly(df, metric, threshold=0.3):
        current_ts = df['ts'].max()  # достаем максимальную 15-минутку из датафрейма - ту, которую будем проверять на аномальность
        day_ago_ts = current_ts - pd.DateOffset(days=1)  # достаем такую же 15-минутку сутки назад

        current_value = df[df['ts'] == current_ts][metric].iloc[0] # достаем из датафрейма значение метрики в максимальную 15-минутку
        day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0] # достаем из датафрейма значение метрики в такую же 15-минутку сутки назад

        # вычисляем отклонение
        if current_value <= day_ago_value:
            diff = abs(current_value / day_ago_value - 1)
        else:
            diff = abs(day_ago_value / current_value - 1)

        if diff > threshold:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, current_value, diff

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220620',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }


default_args = {
    'owner': 'd.kostarev',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 7, 4)
}

schedule_interval = '*/15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kostarev_system_alert_ver_1_dag():
    
    @task
    def extarct_active_users_feed(connection):
        query = """
        SELECT toStartOfFifteenMinutes(time) AS ts ,
               toDate(ts) AS date ,
               formatDateTime(ts, '%R') AS hm ,
               uniqExact(user_id) AS users_feed
        FROM simulator_20220620.feed_actions
        WHERE ts >= today() - 1
          AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts
        """
        data = ph.read_clickhouse(query, connection=connection)

        return data
    
    @task
    def extract_active_users_messanger(connection):
        query = """
        SELECT toStartOfFifteenMinutes(time) AS ts ,
               toDate(ts) AS date ,
               formatDateTime(ts, '%R') AS hm ,
               uniqExact(user_id) AS users_messanger
        FROM simulator_20220620.message_actions
        WHERE ts >= today() - 1
          AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts
        """
        data = ph.read_clickhouse(query, connection=connection)

        return data
    
    @task
    def extract_post_metrics(connection):
        query = """
        SELECT toStartOfFifteenMinutes(time) AS ts ,
               toDate(ts) AS date ,
               formatDateTime(ts, '%R') AS hm ,
               countIf(action='like') AS likes,
               countIf(action='view') AS views,
               countIf(action='like')/countIf(action='view') AS ctr
        FROM simulator_20220620.feed_actions
        WHERE ts >= today() - 1
          AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts
        """
        data = ph.read_clickhouse(query, connection=connection)

        return data
    
    
    @task
    def extract_send_messages(connection):
        query = """
        SELECT toStartOfFifteenMinutes(time) AS ts ,
               toDate(ts) AS date ,
               formatDateTime(ts, '%R') AS hm ,
               count(user_id) AS cnt_send_messages
        FROM simulator_20220620.message_actions
        WHERE ts >= today() - 1
          AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts
        """
        data = ph.read_clickhouse(query, connection=connection)

        return data
    
    
    @task
    def run_alerts(active_users_feed, active_users_messanger, post_metrics, send_messages):
        chat_id = 275002908
        bot = telegram.Bot(token='5595853879:AAEMGPHw1eHFxetHl2dbEZ8tOiFVt0otJyk')

        #users_feed
        metric = 'users_feed'
        is_alert, current_value, diff = check_anomaly(df=active_users_feed, metric=metric, threshold=0.2)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nотклонение от вчера {diff:.2%} \
            https://superset.lab.karpov.courses/superset/dashboard/1125/'''.format(metric=metric, current_value=current_value, diff=diff)
            bot.sendMessage(chat_id=chat_id, text=msg)

        #likes    
        metric = 'likes'
        is_alert, current_value, diff = check_anomaly(df=post_metrics, metric=metric, threshold=0.35)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nотклонение от вчера {diff:.2%} \
            https://superset.lab.karpov.courses/superset/dashboard/1125/'''.format(metric=metric, current_value=current_value, diff=diff)
            bot.sendMessage(chat_id=chat_id, text=msg)

        #views    
        metric = 'views'
        is_alert, current_value, diff = check_anomaly(df=post_metrics, metric=metric, threshold=0.35)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nотклонение от вчера {diff:.2%} \
            https://superset.lab.karpov.courses/superset/dashboard/1125/'''.format(metric=metric, current_value=current_value, diff=diff)
            bot.sendMessage(chat_id=chat_id, text=msg)

        #ctr    
        metric = 'ctr'
        is_alert, current_value, diff = check_anomaly(df=post_metrics, metric=metric, threshold=0.2)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.3f}\nотклонение от вчера {diff:.2%} \
            https://superset.lab.karpov.courses/superset/dashboard/1125/'''.format(metric=metric, current_value=current_value, diff=diff)
            bot.sendMessage(chat_id=chat_id, text=msg)

        #users_messanger
        metric = 'users_messanger'
        is_alert, current_value, diff = check_anomaly(df=active_users_messanger, metric=metric, threshold=0.4)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nотклонение от вчера {diff:.2%} \
            \n@Dmitriy_Kostarev'''.format(metric=metric, current_value=current_value, diff=diff)

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot( 
                data=active_users_messanger.sort_values(by=['date', 'hm']), 
                x="hm", y=metric, 
                hue="date" 
                )
            for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel='') 
            ax.set(ylabel='')
            ax.tick_params(axis='both', which='major', labelsize=16)

            ax.set_title('{}'.format(metric), fontsize=22) 
            ax.set(ylim=(0, None))

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        #cnt_send_messages
        metric = 'cnt_send_messages'
        is_alert, current_value, diff = check_anomaly(df=send_messages, metric=metric, threshold=0.4)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nотклонение от вчера {diff:.2%} \
            '''.format(metric=metric, current_value=current_value, diff=diff)

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot( 
                data=send_messages.sort_values(by=['date', 'hm']), 
                x="hm", y=metric, 
                hue="date" 
                )
            for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel='') 
            ax.set(ylabel='')
            ax.tick_params(axis='both', which='major', labelsize=16)

            ax.set_title('{}'.format(metric), fontsize=22) 
            ax.set(ylim=(0, None))

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    active_users_feed = extarct_active_users_feed(connection)
    active_users_messanger = extract_active_users_messanger(connection)
    post_metrics = extract_post_metrics(connection)
    send_messages = extract_send_messages(connection)
    
    run_alerts(active_users_feed=active_users_feed, active_users_messanger=active_users_messanger, post_metrics=post_metrics, send_messages=send_messages)
    
kostarev_system_alert_ver_1_dag = kostarev_system_alert_ver_1_dag()
