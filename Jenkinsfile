
pipeline {
    agent any

    parameters {
        choice(
            name: 'LOAD_TYPE',
            choices: ['INITIAL', 'INCREMENTAL'],
            description: 'Choose pipeline mode'
        )
    }

    environment {
        REMOTE_HOST = '13.42.152.118'           // EC2 / Spark host
        REMOTE_USER = 'ec2-user'
        CODE_PATH = '/home/ec2-user/praveen_proj/code'
        HADOOP_CONF_DIR = '/etc/hadoop/conf'
        YARN_CONF_DIR = '/etc/hadoop/conf'
    }

    stages {

        stage('Prepare') {
            steps {
                echo "Starting Retail Pipeline Jenkins Build"
            }
        }

        stage('Install Dependencies') {
            steps {
                sh """
                ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                    cd ${CODE_PATH} &&
                    python3 -m pip install -r requirements.txt || true
                '
                """
            }
        }

        stage('Run Pytest Data Quality Checks') {
            steps {
                sh """
                ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                    cd ${CODE_PATH} &&
                    python3 -m pytest -v tests/test_dq.py
                '
                """
            }
        }

        stage('Run Pipeline') {
            steps {
                script {
                    if (params.LOAD_TYPE == 'INITIAL') {
                        sh """
                        ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                            cd ${CODE_PATH} &&
                            export PYSPARK_PYTHON=/usr/bin/python3 &&
                            python3 initial.py
                        '
                        """
                    } else if (params.LOAD_TYPE == 'INCREMENTAL') {
                        sh """
                        ssh -o StrictHostKeyChecking=no ${REMOTE_USER}@${REMOTE_HOST} '
                            cd ${CODE_PATH} &&
                            export PYSPARK_PYTHON=/usr/bin/python3 &&
                            python3 incremental.py
                        '
                        """
                    }
                }
            }
        }
    }

    post {
        success {
            echo "Retail pipeline executed successfully with LOAD_TYPE=${params.LOAD_TYPE}"
        }
        failure {
            echo "Retail pipeline failed. Check Jenkins console logs."
        }
    }
}
