
import time
import json
import random
from kafka import KafkaProducer

def generate_student_data():
    # Factores sociodemográficos y laborales simulados
    estratos = [1, 2, 3, 4]
    situacion_laboral = ['Trabaja tiempo completo', 'Trabaja medio tiempo', 'No trabaja', 'Independiente']
    horas_laborales = [0, 20, 40, 48]
    cargas_academicas = [3, 4, 5, 6] # Número de materias
    
    estrato = random.choice(estratos)
    laboral = random.choice(situacion_laboral)
    horas = 40 if laboral == 'Trabaja tiempo completo' else (20 if laboral == 'Trabaja medio tiempo' else 0)
    materias = random.choice(cargas_academicas)
    
    # Lógica simulada de cálculo de riesgo de deserción (0.0 a 1.0)
    # Mayor riesgo si es de estrato bajo, trabaja muchas horas y tiene mucha carga académica
    riesgo_base = 0.1
    if estrato in [1, 2]: riesgo_base += 0.3
    if horas >= 40: riesgo_base += 0.35
    elif horas == 20: riesgo_base += 0.15
    if materias >= 5: riesgo_base += 0.2
    
    riesgo_desercion = round(min(riesgo_base + random.uniform(-0.05, 0.05), 1.0), 2)

    return {
        "student_id": random.randint(1000, 5000),
        "estrato": estrato,
        "situacion_laboral": laboral,
        "horas_laborales": horas,
        "carga_academica": materias,
        "riesgo_desercion": riesgo_desercion,
        "timestamp": int(time.time())
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    student_data = generate_student_data()
    producer.send('sensor_data', value=student_data)
    print(f"Enviado: {student_data}")
    time.sleep(1)
