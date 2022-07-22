import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
import io
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import numpy as np

connection = connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220620',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }
        


#проверяем вхождение текущего значения в доверительный интервал посчитанный при помощи межквартильного размаха
def check_anomaly(df, metric, alpha=1.5):
    current_ts = df['ts'].max()
    current_value = df[df['ts'] == current_ts][metric].iloc[0]
    lst_prev_values = df.sort_values('ts')[metric].to_list()[:-1] #получаем значения метрик за предыдущий период
    IQR = np.quantile(lst_prev_values, 0.75) - np.quantile(lst_prev_values, 0.25) #определяем межквартильный размах
    left = np.quantile(lst_prev_values, 0.25) - alpha * IQR #определяем левую границу дов.интервала  
    right = np.quantile(lst_prev_values, 0.75) + alpha * IQR #определяем правую границу дов.интервала
    
    if current_value > right:
        is_alert = 1
    elif current_value < left:
        is_alert =  1
    else:
        is_alert = 0
        
    return current_value, is_alert, left, right

default_args = {
    'owner': 'd.kostarev',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 7, 4)
}

schedule_interval = '*/15 * * * *'



@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def kostarev_system_alert_ver_2_dag():
    
    @task    
    def extract_active_users_feed(connection):
        query = """
        SELECT toStartOfFifteenMinutes(time) AS ts ,
               toDate(ts) AS date ,
               formatDateTime(ts, '%R') AS hm ,
               uniqExact(user_id) AS users_feed
        FROM simulator_20220620.feed_actions
        WHERE ts >= now() - INTERVAL 2 HOUR + INTERVAL 15 MINUTE
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
               formatDateTime(ts, '%R') AS hm ,
               uniqExact(user_id) AS users_messanger
        FROM simulator_20220620.message_actions
        WHERE ts >= now() - INTERVAL 2 HOUR + INTERVAL 15 MINUTE
          AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, hm
        ORDER BY ts
        """
        data = ph.read_clickhouse(query, connection=connection)

        return data
    
    @task
    def extract_post_metrics(connection):
        query = """
        SELECT toStartOfFifteenMinutes(time) AS ts ,
               formatDateTime(ts, '%R') AS hm ,
               countIf(action='like') AS likes,
               countIf(action='view') AS views,
               countIf(action='like')/countIf(action='view') AS ctr
        FROM simulator_20220620.feed_actions
        WHERE ts >= now() - INTERVAL 2 HOUR + INTERVAL 15 MINUTE
          AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, hm
        ORDER BY ts
        """
        data = ph.read_clickhouse(query, connection=connection)

        return data
    
    @task
    def extract_send_messages(connection):
        query = """
        SELECT toStartOfFifteenMinutes(time) AS ts ,
               formatDateTime(ts, '%R') AS hm ,
               count(user_id) AS cnt_send_messages
        FROM simulator_20220620.message_actions
        WHERE ts >= now() - INTERVAL 2 HOUR + INTERVAL 15 MINUTE
          AND ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, hm
        ORDER BY ts
        """
        data = ph.read_clickhouse(query, connection=connection)

        return data
        
        
        
    
    @task
    def run_alerts(active_users_feed, post_metrics, active_users_messanger, send_messages):
        chat_id = -519115532
        bot = telegram.Bot(token='5595853879:AAEMGPHw1eHFxetHl2dbEZ8tOiFVt0otJyk')

        #users_feed
        metric = 'users_feed'
        current_value, is_alert, left, right = check_anomaly(df=active_users_feed, metric=metric, alpha=5)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nне входит в доверительный интервал предыдущего часа \n({left:.0f} : {right:.0f}) \
            '''.format(metric=metric, current_value=current_value, left=left, right=right)
            
            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot( 
                data=active_users_feed, x="hm", y=metric, 
                )
            y_max = active_users_feed[metric].max()*1.3
            ax.set(xlabel='') 
            ax.set(ylabel='')
            ax.tick_params(axis='both', which='major', labelsize=22)

            ax.set_title('{}'.format(metric), fontsize=22) 
            ax.set(ylim=(0, y_max))
            for x,y,m in active_users_feed[['hm','users_feed','users_feed']].values:
                ax.text(x,y,f'{m:.0f}',fontsize=22);

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)      

        #views
        metric = 'views'
        current_value, is_alert, left, right = check_anomaly(df=post_metrics, metric=metric, alpha=5)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nне входит в доверительный интервал предыдущего часа \n({left:.0f} : {right:.0f}) \
            '''.format(metric=metric, current_value=current_value, left=left, right=right)
            
            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot( 
                data=post_metrics, x="hm", y=metric, 
                )
            y_max = post_metrics[metric].max()*1.3
            ax.set(xlabel='') 
            ax.set(ylabel='')
            ax.tick_params(axis='both', which='major', labelsize=22)

            ax.set_title('{}'.format(metric), fontsize=22) 
            ax.set(ylim=(0, y_max))
            for x,y,m in post_metrics[['hm','views','views']].values:
                ax.text(x,y,f'{m:.0f}',fontsize=22);

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        #likes
        metric = 'likes'
        current_value, is_alert, left, right = check_anomaly(df=post_metrics, metric=metric, alpha=5)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nне входит в доверительный интервал предыдущего часа \n({left:.0f} : {right:.0f}) \
            '''.format(metric=metric, current_value=current_value, left=left, right=right)
            
            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot( 
                data=post_metrics, x="hm", y=metric, 
                )
            y_max = post_metrics[metric].max()*1.3
            ax.set(xlabel='') 
            ax.set(ylabel='')
            ax.tick_params(axis='both', which='major', labelsize=22)

            ax.set_title('{}'.format(metric), fontsize=22) 
            ax.set(ylim=(0, y_max))
            for x,y,m in post_metrics[['hm','likes','likes']].values:
                ax.text(x,y,f'{m:.0f}',fontsize=22);

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)       

        #ctr
        metric = 'ctr'
        current_value, is_alert, left, right = check_anomaly(df=post_metrics, metric=metric, alpha=10)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:.3f}\nне входит в доверительный интервал предыдущего часа \n({left:.3f} : {right:.3f}) \
            '''.format(metric=metric, current_value=current_value, left=left, right=right)

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot( 
                data=post_metrics, x="hm", y=metric, 
                )
            y_max = post_metrics[metric].max()*1.3
            ax.set(xlabel='') 
            ax.set(ylabel='')
            ax.tick_params(axis='both', which='major', labelsize=22)

            ax.set_title('{}'.format(metric), fontsize=22) 
            ax.set(ylim=(0, y_max))
            for x,y,m in post_metrics[['hm','ctr','ctr']].values:
                ax.text(x,y,f'{m:.3f}', fontsize=22);

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        #users_messanger
        metric = 'users_messanger'
        current_value, is_alert, left, right = check_anomaly(df=active_users_messanger, metric=metric, alpha=10)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nне входит в доверительный интервал предыдущего часа \n({left:.0f} : {right:.0f}) \
            '''.format(metric=metric, current_value=current_value, left=left, right=right)

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot( 
                data=active_users_messanger, x="hm", y=metric, 
                )
            y_max = active_users_messanger[metric].max()*1.3
            ax.set(xlabel='') 
            ax.set(ylabel='')
            ax.tick_params(axis='both', which='major', labelsize=22)

            ax.set_title('{}'.format(metric), fontsize=22) 
            ax.set(ylim=(0, y_max))
            for x,y,m in active_users_messanger[['hm','users_messanger','users_messanger']].values:
                ax.text(x,y,f'{m:.0f}', fontsize=22);

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        #cnt_send_messages
        metric = 'cnt_send_messages'
        current_value, is_alert, left, right = check_anomaly(df=send_messages, metric=metric, alpha=10)
        if is_alert:
            msg = '''Метрика {metric}:\nтекущее значение = {current_value:}\nне входит в доверительный интервал предыдущего часа \n({left:.0f} : {right:.0f}) \
            '''.format(metric=metric, current_value=current_value, left=left, right=right)

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot( 
                data=send_messages, x="hm", y=metric, 
                )
            y_max = send_messages[metric].max()*1.3
            ax.set(xlabel='') 
            ax.set(ylabel='')
            ax.tick_params(axis='both', which='major', labelsize=22)

            ax.set_title('{}'.format(metric), fontsize=22) 
            ax.set(ylim=(0, y_max))
            for x,y,m in send_messages[['hm','cnt_send_messages','cnt_send_messages']].values:
                ax.text(x,y,f'{m:.0f}', fontsize=22);

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
    active_users_feed = extract_active_users_feed(connection)
    post_metrics = extract_post_metrics(connection) 
    active_users_messanger = extract_active_users_messanger(connection) 
    send_messages = extract_send_messages(connection)
    
    run_alerts(active_users_feed=active_users_feed, active_users_messanger=active_users_messanger, post_metrics=post_metrics, send_messages=send_messages)
    
kostarev_system_alert_ver_2_dag = kostarev_system_alert_ver_2_dag()