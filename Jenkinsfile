
pipeline {
    agent any

    parameters {
        choice(
            name: 'MODE',
            choices: ['initial', 'incremental'],
            description: 'Select pipeline mode'
        )
    }

    environment {
        SPARK_HOME = "/usr/lib/spark"
    }

    stages {
        stage('Checkout') {
            steps {
                git branch: 'main', url: 'https://github.com/praveenkumarmuthukaruppan/praveen_proj.git'
            }
        }

        stage('Run Pipeline') {
            steps {
                script {
                    if (params.MODE == 'initial') {
                        echo "Running INITIAL pipeline"
                        sh "${SPARK_HOME}/bin/spark-submit initial.py"
                    } else {
                        echo "Running INCREMENTAL pipeline"
                        sh "${SPARK_HOME}/bin/spark-submit incremental.py"
                    }
                }
            }
        }
    }

    post {
        success {
            echo "Pipeline completed successfully "
        }
        failure {
            echo "Pipeline failed"
        }
    }
}
