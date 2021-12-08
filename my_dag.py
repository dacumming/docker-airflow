from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime

def send_path(**kwargs):
    import os
    import sqlalchemy
    cwd = os.getcwd()
    dir = os.path.join(cwd,'dags/fma_small')
    files_dirs=[]
    for path, subdirs, files in os.walk(dir):
        for name in files:
            string=os.path.join(path, name)
            if '.mp3' in string:
                files_dirs.append(string)
    pg_hook=PostgresHook(postgres_conn_id="postgres_conn", schema='airflow')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as connection:
        resultproxy = connection.execute('select cnt from count')
    cnt = [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy][0]['cnt']
    print(cnt)
    return files_dirs[cnt]
    
    
def get_spec_track(**kwargs):
    import os
    import librosa
    from librosa import display
    import matplotlib.pyplot as plt
    import numpy as np
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='fetch_track')
    
    sig, fs = librosa.load(os.fspath(ls))
    S = librosa.feature.melspectrogram(y=sig, sr=fs)

    fig = plt.figure(figsize=(8,8), tight_layout=True)
    plt.axis('off')
    ax = fig.add_subplot(111)
    p = librosa.display.specshow(librosa.amplitude_to_db(S, ref=np.max), ax=ax, y_axis='log', x_axis='time')
    if not os.path.exists('/usr/local/airflow/dags/spectrograms/'+str(os.path.splitext(os.path.basename(ls))[0])):
        try:
            os.makedirs('/usr/local/airflow/dags/spectrograms/'+str(os.path.splitext(os.path.basename(ls))[0]))
        except:
            pass
    ax.axes.xaxis.set_visible(False)
    ax.axes.yaxis.set_visible(False)
    fig.savefig('/usr/local/airflow/dags/spectrograms/'+str(os.path.splitext(os.path.basename(ls))[0])+'/track.png')
    
def get_spec_features(**kwargs):
    import os
    import librosa
    from librosa import display
    import matplotlib.pyplot as plt
    import numpy as np
    ti = kwargs['ti']
    feature=kwargs['feature']
    ls = ti.xcom_pull(task_ids='fetch_track')
    
    sig, fs = librosa.load(os.fspath('/usr/local/airflow/dags/separations/'+str(os.path.splitext(os.path.basename(ls))[0])+'/'+feature+'.mp3'))
    
    S = librosa.feature.melspectrogram(y=sig, sr=fs)

    fig = plt.figure(figsize=(8,8), tight_layout=True)
    plt.axis('off')
    ax = fig.add_subplot(111)
    p = librosa.display.specshow(librosa.amplitude_to_db(S, ref=np.max), ax=ax, y_axis='log', x_axis='time')
    if not os.path.exists('/usr/local/airflow/dags/spectrograms/'+str(os.path.splitext(os.path.basename(ls))[0])):
        try:
            os.makedirs('/usr/local/airflow/dags/spectrograms/'+str(os.path.splitext(os.path.basename(ls))[0]))
        except:
            pass
    ax.axes.xaxis.set_visible(False)
    ax.axes.yaxis.set_visible(False)
    fig.savefig('/usr/local/airflow/dags/spectrograms/'+str(os.path.splitext(os.path.basename(ls))[0])+'/'+feature+'.png')

def update_db(**kwargs):
    import sqlalchemy
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='fetch_track')
    subfolder="/".join(ls.split("/", -2)[-2:])
    track_id="/".join(ls.split("/", -1)[-1:]).split(".")[0]
    host_dir='C:/Users/asus/Documents/GitHub/airflow-docker/dags/'
    pg_hook=PostgresHook(postgres_conn_id="postgres_conn", schema='airflow')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as connection:

        connection.execute("UPDATE tracks SET track='%s', bass='%s', drums='%s', other='%s', vocals='%s', track_spec='%s', bass_spec='%s', drums_spec='%s', other_spec='%s', vocals_spec='%s' WHERE track_id=%s" % (host_dir+'fma_small/'+subfolder,
                                                    host_dir+'separations/'+track_id+'/bass.mp3',
                                                    host_dir+'separations/'+track_id+'/drums.mp3',
                                                    host_dir+'separations/'+track_id+'/other.mp3',
                                                    host_dir+'separations/'+track_id+'/vocals.mp3',
                                                    host_dir+'spectrograms/'+track_id+'/track.png',
                                                    host_dir+'spectrograms/'+track_id+'/bass.png',
                                                    host_dir+'spectrograms/'+track_id+'/drums.png',
                                                    host_dir+'spectrograms/'+track_id+'/other.png',
                                                    host_dir+'spectrograms/'+track_id+'/vocals.png',
                                                    str(int(track_id))))
        connection.execute('UPDATE count SET cnt=cnt+1')


with DAG("my_dag", start_date=datetime(2021, 12, 4), 
    schedule_interval="* * * * *", catchup=False,max_active_runs=1) as dag:
    

    fetch_track = PythonOperator(
        task_id="fetch_track",
        python_callable=send_path,
        provide_context=True
    )

    run_demucs = BashOperator(
        task_id='run_demucs',
        bash_command="separation=$(python3 -m demucs --mp3 {{ ti.xcom_pull(task_ids='fetch_track') }} | awk '/Separated/ {print $7}') && yes | cp -rf $separation/* /usr/local/airflow/dags/separations/",
        provide_context=True
    )

    get_spectrogram_track = PythonOperator(task_id='get_spec_track',
                            python_callable=get_spec_track,
                            provide_context=True)

    get_spectrograms_features = [PythonOperator(task_id=f'get_spec_{ft}',
                                python_callable=get_spec_features,
                                op_kwargs={'feature': ft},
                                provide_context=True
                                ) for ft in ['bass', 'drums', 'other', 'vocals']]
    update_data = PythonOperator(task_id='update_db',
                            python_callable=update_db,
                            provide_context=True,
                            trigger_rule='all_success')

    fetch_track >> run_demucs >> get_spectrograms_features+[get_spectrogram_track] >> update_data

    # inst = BashOperator(
    #     task_id='inst',
    #     bash_command="pip install soundfile"
    # )
    # inst
    