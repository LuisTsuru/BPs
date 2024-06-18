from hcsr04 import HCSR04
from machine import Pin, PWM
from iot_data_hub import IoTDataHub
import time

# Declaracao de variaveis
alerta = Pin(23,Pin.OUT)             # Pisca Alerta
buzzer = PWM(Pin(5), duty=512)       # Buzzer
reset = Pin(27,Pin.IN,Pin.PULL_DOWN) # Botao reset
sensor = HCSR04(12, 14)              # Sensor de batimentos

# Conectividade
servidor = IoTDataHub(
    "Wokwi-GUEST",
    "",
    "JE5bJ325zTV8QKn8wbPLABCDF",
    verbose=True
)

# Funcao para Buzzer
def beep():
    buzzer.init()
    time.sleep(0.1)
    buzzer.deinit()
    time.sleep(0.1)

# Funcao para alertar
def perigo():
    beep()
    alerta.on()
    time.sleep(.1)
    beep()
    alerta.off()
    time.sleep(.1)


# Main
while True:
    buzzer.deinit()
    batimento =  int(sensor.distance_cm())
    servidor.publish("HeartBeat", str(batimento) )
    #Confere se os batimentos estao na area de risco
    if batimento > 150:
        # Emitira um aletar ate que o dispositivo seja reiniciado
        while True:
            perigo()
            if sensor.distance_cm() > batimento:
                batimento =  int(sensor.distance_cm())
            servidor.publish("HeartAtack", str(batimento) + " Bps")
            if reset.value():
                print("Reset")
                break
            time.sleep(.1)
    time.sleep(1)
