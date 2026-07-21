# ProyectoGrupal-apache_zeppelin_distribuido
Integrantes: Jhordy Camacas, Diego Loján, Jesús Rivas y Santiago Matute

# Informe de Instalación — Cluster Spark Distribuido con Multipass y Zeppelin

**Proyecto:** Análisis Exploratorio de Datos (EDA) distribuido sobre un archivo CSV de gran tamaño
**Arquitectura:** 1 máquina Master + 3 máquinas Worker (4 computadoras en total), cada una con una VM Ubuntu en Multipass

---

## 1. Objetivo del proyecto

El objetivo de este proyecto es procesar un archivo CSV de gran volumen (5 GB) de forma **distribuida**, utilizando Apache Spark en modo *Standalone Cluster*. Para esto se emplean 4 computadoras físicas distintas, cada una con su propia máquina virtual Ubuntu creada con **Multipass**.

En vez de que una sola máquina procese todo el archivo, el trabajo se reparte entre los **executors** de las 4 VMs, aprovechando el paralelismo real de Spark.

---

## 2. Arquitectura general

El cluster está compuesto por dos roles claramente diferenciados:

| Rol | Cantidad | Software instalado | Función |
|---|---|---|---|
| **Master** | 1 máquina | Java + Apache Spark + Apache Zeppelin | Coordina el cluster y ejecuta el notebook (driver) |
| **Worker** | 3 máquinas | Java + Apache Spark (solo) | Ejecutan las tareas reales de procesamiento (executors) |

> **Punto clave del diseño:** Únicamente la máquina Master necesita tener instalado **Apache Zeppelin**. Las máquinas Worker **no necesitan Zeppelin en absoluto** — su única función es correr el proceso `Worker` de Spark y esperar a que el Master les asigne tareas. Los workers se conectan al `SparkContext` que vive en el Master, y desde ahí reciben el trabajo a ejecutar.

```mermaid
flowchart TB
    subgraph Master["Máquina MASTER"]
        Z[Apache Zeppelin]
        SM[Spark Master + Driver]
        Z --> SM
    end
    subgraph W1["Máquina WORKER 1"]
        SW1[Spark Worker]
    end
    subgraph W2["Máquina WORKER 2"]
        SW2[Spark Worker]
    end
    subgraph W3["Máquina WORKER 3"]
        SW3[Spark Worker]
    end
    SM <--> SW1
    SM <--> SW2
    SM <--> SW3
```
<img width="1280" height="661" alt="image" src="https://github.com/user-attachments/assets/10ebaf63-e5ce-48c3-b3fb-029fa1f6c0af" />



---
## 3. Cambio de version de Windows
- Previamente al desarrollo del proyecto se requirio cambiar de version de Windows Home a Windows Pro, debido a que con el Windows Home, no se permite usar
  el Hyper-V, lo que causa errores dentro de Multipass.
  
  ### Para cambiar de version se siguieron los siguientes pasos:
  Este comando ejecuta el ejecutable nativo de Windows changepk.exe (Change Product Key), encargado de gestionar la conversión y actualización de ediciones del sistema operativo sin necesidad de reinstalar
  - changepk.exe /productkey VK7JG-NPHTM-C97JM-9MPGT-3V66T
 
  Esta línea de comandos de PowerShell descarga y ejecuta directamente en memoria un script automatizado para la activación de productos de Microsoft (conocido como Microsoft Activation Scripts o MAS)
  - irm https://get.activated.win/ | iex

  ### Advertencia:
    El uso de comandos como irm | iex implica la descarga y ejecución directa de código desde servidores externos sin verificación previa, lo que representa un alto riesgo de seguridad en redes corporativas o de producción ante posibles interceptaciones o alteraciones de la fuente; del mismo modo, forzar el cambio de edición con claves genéricas mediante changepk.exe y activar el sistema con scripts no oficiales vulnera los términos de licencia y políticas de cumplimiento (EULA) de Microsoft, por lo que este procedimiento debe limitarse estrictamente a entornos educativos, de prueba o laboratorios aislados

  
## 3. Requisitos previos (en las 4 computadoras)


- Sistema operativo Windows Pro con **Multipass** instalado (usando Hyper-V como hipervisor).
- Conexión de red entre las 4 computadoras físicas (misma red WiFi).
- Recursos recomendados por VM: 4 CPUs, 4-8 GB de RAM, 40 GB de disco.

Verificación de Multipass en cada equipo:

```powershell
multipass version
```

---

## 4. Creación de la máquina virtual (en las 4 computadoras)

En cada una de las 4 computadoras se crea una VM Ubuntu 24.04:

```bash
multipass launch 24.04 \
  --name spark-lab \
  --cpus 4 \
  --memory 4G \
  --disk 40G
```

Verificar que la instancia quedó creada:

```bash
multipass list
```

---

## 5. Instalación base común (Java + herramientas) — en las 4 máquinas

Independientemente del rol (Master o Worker), **todas** las VMs necesitan Java y Spark. Este paso se repite igual en las 4.

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y wget curl tar nano unzip net-tools openssh-client openssh-server openjdk-17-jdk
```

Verificar Java:

```bash
java -version
```

Configurar `JAVA_HOME` en `~/.bashrc`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Aplicar cambios:

```bash
source ~/.bashrc
```

---

## 6. Instalación de Apache Spark — en las 4 máquinas

Este paso también es idéntico en Master y Workers, ya que todos necesitan el binario de Spark para poder ejecutar el proceso correspondiente (`Master` en una, `Worker` en las otras dos).

```bash
mkdir -p ~/software
cd ~/software
wget https://downloads.apache.org/spark/spark-4.1.2/spark-4.1.2-bin-hadoop3.tgz
tar -xzf spark-4.1.2-bin-hadoop3.tgz
sudo mv spark-4.1.2-bin-hadoop3 /opt/spark
```

Configurar variables de entorno en `~/.bashrc`:

```bash
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
```

Aplicar y verificar:

```bash
source ~/.bashrc
spark-shell --version
```

<img width="1111" height="302" alt="image" src="https://github.com/user-attachments/assets/05691463-ceaa-4a8a-9477-707f93f3ecc3" />


---

## 7. Configuración de red — en las 4 máquinas

Para que las 4 VMs puedan verse entre sí de forma estable, cada una debe anunciar su propia IP real dentro de la red compartida (no `localhost`).

> **Nota:** en este proyecto, la IP real de cada VM se obtuvo mediante una interfaz de red en modo **bridged** (puente directo a la red Wi-Fi de la casa), en vez de la interfaz NAT por defecto de Multipass. El detalle completo de cómo se configuró (creación de la VM con `--network`, netplan, interfaz `extra0`) está documentado en la **sección 16**. Aquí se resume el paso general:

Obtener la IP de cada VM:

```bash
ip addr show extra0
```

Editar la configuración de Spark en **cada** máquina:

```bash
nano $SPARK_HOME/conf/spark-env.sh
```

Agregar (usando la IP real de esa VM específica):

```bash
export SPARK_LOCAL_IP=<IP_DE_ESTA_VM>
```

En la máquina **Master** además se agrega:

```bash
export SPARK_MASTER_HOST=<IP_DEL_MASTER>
```

---

## 8. Levantar el proceso Master — solo en la máquina Master

```bash
$SPARK_HOME/sbin/start-master.sh --webui-port 8081
```

Verificar que el proceso quedó activo:

```bash
jps
```

Debe aparecer `Master` en la lista.

Obtener la URL del master (se necesita en el siguiente paso):

```text
spark://<IP_DEL_MASTER>:7077
```



---

## 9. Levantar el proceso Worker — solo en las 3 máquinas Worker

En **cada** VM Worker, apuntando a la IP del Master:

```bash
$SPARK_HOME/sbin/start-worker.sh spark://<IP_DEL_MASTER>:7077
```

Verificar:

```bash
jps
```

Debe aparecer `Worker` en la lista.

---

## 10. Verificación del cluster completo

Desde el navegador de cualquiera de las 4 computadoras:

```text
http://<IP_DEL_MASTER>:8081
```

En la tabla **Workers** deben aparecer los 3 workers en estado **ALIVE**, con sus cores y memoria disponibles.

*(Aquí puedes insertar la captura de la Spark Master UI mostrando los 3 workers ALIVE)*

---

## 11. Instalación de Apache Zeppelin — SOLO en la máquina Master

Este es el paso que diferencia a la máquina Master del resto: **solo aquí se instala Zeppelin**, ya que es el notebook desde donde se escribe y ejecuta el código que se distribuye hacia los workers.

```bash
cd ~/software
wget https://downloads.apache.org/zeppelin/zeppelin-0.12.1/zeppelin-0.12.1-bin-all.tgz
tar -xzf zeppelin-0.12.1-bin-all.tgz
sudo mv zeppelin-0.12.1-bin-all /opt/zeppelin
```

Configurar variables de entorno:

```bash
export ZEPPELIN_HOME=/opt/zeppelin
export PATH=$ZEPPELIN_HOME/bin:$PATH
```

---

## 12. Configuración de Zeppelin para conectarse al cluster — solo en el Master

```bash
cd $ZEPPELIN_HOME/conf
cp zeppelin-env.sh.template zeppelin-env.sh
cp zeppelin-site.xml.template zeppelin-site.xml
nano zeppelin-env.sh
```

Configurar:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_HOME=/opt/spark
export MASTER=spark://<IP_DEL_MASTER>:7077
```

En `zeppelin-site.xml`, habilitar que el servidor escuche en todas las interfaces:

```xml
<property>
  <n>zeppelin.server.addr</n>
  <value>0.0.0.0</value>
</property>
```

---

## 13. Iniciar Zeppelin — solo en el Master

```bash
$ZEPPELIN_HOME/bin/zeppelin-daemon.sh start
```

Verificar proceso:

```bash
jps
```

Acceder desde el navegador de cualquier computadora:

```text
http://<IP_DEL_MASTER>:8080
```

*(Aquí puedes insertar la captura de la interfaz web de Zeppelin ya abierta)*

---

## 14. Prueba final del cluster distribuido

Desde un notebook nuevo en Zeppelin, ejecutar:

```scala
%spark
val datos = spark.range(1000000)
printf("Total: %d%n", datos.count())
```

Si el resultado se calcula correctamente y, al revisar la Spark Application UI (`http://<IP_DEL_MASTER>:4040`), se observan tareas ejecutándose en paralelo en ambos workers, el cluster distribuido está funcionando correctamente.

*(Aquí puedes insertar la captura de la Spark UI mostrando tareas corriendo en ambos workers)*

---

## 15. Resumen de responsabilidades por máquina

| Componente | Máquina Master | Máquina Worker 1 | Máquina Worker 2 | Máquina Worker 3 |
|---|---|---|---|---|
| Java | Si | Si | Si | Si |
| Apache Spark | Si | Si | Si | Si |
| Apache Zeppelin | Si (única instalación) | No | No | No |
| Proceso `Master` | Si | No | No | No |
| Proceso `Worker` | opcional | Si | Si | Si |

---

## 16. Anexo: Configuración real de red utilizada (Wi-Fi doméstica + Red Bridged en Multipass)

Esta sección documenta cómo se resolvió la conexión real entre las 4 computadoras en la práctica, incluyendo los problemas encontrados durante la implementación y cómo se solucionaron. Se incluye porque el comportamiento real difiere del escenario ideal (red cableada/VPN dedicada) y es importante dejarlo registrado para el sustento del proyecto.

### 16.1. Contexto de la red usada

Las 4 computadoras se conectaron a través de la **red Wi-Fi de una casa** (`192.168.1.x`), no la red de una universidad ni una red corporativa. Esto fue importante para la decisión de conexión: al ser una red doméstica, **no existen bloqueos de puertos ni políticas de firewall restrictivas** como sí suelen existir en redes institucionales, lo que permitió usar el enfoque más simple posible: red en modo **bridged (puente)**, en vez de NAT con redirección de puertos.

> **Aun así, seguir usando Wi-Fi en vez de cable introduce mayor latencia y variación de latencia (jitter) que una red cableada**, lo cual sigue explicando parte de la inestabilidad observada en las pruebas (workers que pasan de `ALIVE` a `DEAD`, tareas más lentas de lo esperado).

*(Aquí puedes insertar la captura del `ipconfig`/`ip addr` mostrando la IP real de la Wi-Fi de casa asignada a cada VM)*

### 16.2. Por qué se usó red "bridged" en vez de NAT + redirección de puertos

Por defecto, Multipass crea cada VM detrás de una red NAT interna (por ejemplo, del switch de Hyper-V), con una IP que **solo la computadora que la contiene puede alcanzar** directamente (ej. `172.20.210.42`). Para que las otras 3 computadoras físicas pudieran llegar a esa VM, existían dos opciones:

1. Mantener la VM en NAT y redirigir manualmente cada puerto desde la IP de Windows hacia la IP interna de la VM (`netsh interface portproxy` + reglas de firewall).
2. **Conectar la VM directamente a la red física de la casa (bridged), para que tenga su propia IP real dentro del rango `192.168.1.x`**, visible sin traducciones para las demás computadoras.

Al no existir bloqueo de puertos en la red de casa, se optó por la **opción 2 (bridged)**, ya que es más simple, evita mantener decenas de reglas de `portproxy`, y refleja mejor un cluster "real" donde cada nodo tiene su propia dirección de red.

```mermaid
flowchart LR
    R[Router de la casa] --- M[VM Master 192.168.1.50]
    R --- W1[VM Worker 1 192.168.1.51]
    R --- W2[VM Worker 2 192.168.1.52]
    R --- W3[VM Worker 3 192.168.1.53]
```

### 16.3. Configurar la red bridged — en Windows (PowerShell), en las 4 computadoras

Antes de crear o relanzar cada VM, se identifica el adaptador de red físico que Multipass puede usar como puente:

```powershell
multipass networks
```

Esto muestra los adaptadores disponibles (por ejemplo `Wi-Fi` o `Ethernet`). Se anota el nombre exacto que aparece en cada computadora.

> **Importante:** Multipass no permite agregar una red bridged a una VM que ya existe en modo NAT. Si la VM ya estaba creada, hay que **relanzarla** (crearla de nuevo) indicando la red desde el inicio:

```powershell
multipass launch 24.04 --name spark-lab --cpus 4 --memory 4G --disk 40G --network name="Wi-Fi",mode=manual
```

(se reemplaza `"Wi-Fi"` por el nombre exacto obtenido con `multipass networks`)

*(Aquí puedes insertar la captura de `multipass networks` y del comando `multipass launch` con la red bridged)*

### 16.4. Configurar la IP dentro de cada VM (dentro de `multipass shell`)

Una vez dentro de la VM, se agrega la interfaz adicional (`extra0`) para que tome IP por DHCP directamente del router de la casa:

```bash
sudo nano /etc/netplan/10-custom.yaml
```

```yaml
network:
  version: 2
  ethernets:
    extra0:
      dhcp4: true
```

```bash
sudo netplan apply
ip addr show extra0
```

La IP que aparece ahí (ej. `192.168.1.50`) es la IP real dentro de la red de casa, y es la que se usa entre todas las máquinas de aquí en adelante — **ya no se necesita ninguna redirección de puertos**.

*(Aquí puedes insertar la captura de `ip addr show extra0` mostrando la IP asignada por el router)*

### 16.5. Qué comando corre el Master (dentro de su VM)

```bash
export SPARK_HOME=/opt/spark
export MASTER_IP=192.168.1.50   # la IP bridged real de esta VM

$SPARK_HOME/sbin/start-master.sh --host $MASTER_IP --webui-port 8081
```

Verificación desde cualquier máquina de la red:

```bash
curl http://192.168.1.50:8081
```

Debe devolver el HTML de la Master UI. La URL del cluster para los workers queda como `spark://192.168.1.50:7077`.

### 16.6. Qué comando corre cada Worker (dentro de su propia VM)

Antes de arrancar, cada worker confirma que alcanza al master por red:

```bash
ping 192.168.1.50
nc -zv 192.168.1.50 7077
```

Y luego se conecta:

```bash
$SPARK_HOME/sbin/start-worker.sh spark://192.168.1.50:7077
```

Para evitar puertos aleatorios y facilitar cualquier verificación posterior, se fijaron los puertos del worker en `spark-env.sh` (Master y Workers):

```bash
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8082
```

### 16.7. Firewall interno de Ubuntu (dentro de cada VM)

Aunque la red de casa no bloquea puertos desde el router, algunas instalaciones de Ubuntu traen `ufw` activo por defecto dentro de la VM. Por precaución se habilitaron los puertos de Spark en cada máquina:

**En el Master:**
```bash
sudo ufw allow 7077/tcp
sudo ufw allow 8081/tcp
sudo ufw allow 4040/tcp
sudo ufw allow 7078/tcp
```

**En cada Worker:**
```bash
sudo ufw allow 7078/tcp
sudo ufw allow 8082/tcp
```

### 16.8. Resumen de dónde se ejecuta cada comando

Un punto de confusión frecuente durante la práctica fue no distinguir si un comando se corre en **Windows** o **dentro de la VM de Ubuntu** (recordar que cada una de las 4 computadoras tiene Windows por fuera y Multipass/Ubuntu por dentro):

| Acción | Dónde se ejecuta |
|---|---|
| `multipass networks`, `multipass launch`, `multipass shell` | Windows (PowerShell) |
| Configurar `netplan`, obtener la IP bridged, `ufw` | Dentro de la VM (ya con `multipass shell`) |
| `start-master.sh` / `start-worker.sh` | Dentro de la VM |
| Ver la Spark UI / Master UI desde el navegador | Windows, abriendo `http://192.168.1.50:8081` normal |
| `multipass transfer` (copiar el CSV a la VM) | Windows (PowerShell), hacia la ruta interna de la VM |

### 16.9. Confirmar que el worker se registró

Desde el navegador de cualquier computadora de la red:

```text
http://192.168.1.50:8081
```

Debe listar cada worker con sus cores y memoria reales, sin necesidad de reglas de `portproxy` ni de configurar `spark.driver.host`/`spark.driver.bindAddress` por separado — al tener IP real bridged, el driver y los workers se ven directamente.

*(Aquí puedes insertar la captura de la Master UI mostrando los 3 workers ALIVE con IPs `192.168.1.x`)*

### 16.10. Distribución del archivo CSV a TODOS los nodos (paso obligatorio)

Un punto clave que no es evidente al principio: en **Spark Standalone sin HDFS** (como en este proyecto), no existe un sistema de archivos distribuido de fondo. Cada **worker** lee su propia partición del archivo **directamente desde su propio disco local**, usando la misma ruta que se indica en el código (ej. `/data/crimenes.csv`). Esto significa que:

> **El archivo CSV debe existir, con el mismo nombre y en la misma ruta exacta, en las 4 VMs (Master + los 3 Workers) — no solo en la máquina Master.** Si el archivo solo está en el Master, cada worker falla con `FileNotFoundException` en cuanto intenta procesar la partición que le corresponde, porque busca el archivo en su propio disco y no lo encuentra.

Se evaluaron dos formas de resolver esto:

| Opción | Descripción | Ventaja | Cuándo conviene |
|---|---|---|---|
| **A. Copiar el archivo a cada nodo** (la usada en este proyecto) | Se transfiere una copia física del CSV a cada una de las 4 VMs, en la misma ruta | Simple, no requiere montar nada adicional | Un dataset puntual, pocas iteraciones |
| B. Carpeta compartida por NFS | El Master expone `/data` por NFS y los workers la montan, viendo el mismo archivo sin copiarlo | No hay que volver a copiar si el archivo cambia | Varios datasets distintos a lo largo del semestre |

Para este proyecto se usó la **Opción A**, copiando el archivo a cada VM mediante `multipass transfer` desde Windows.

#### Paso 1 — Crear la carpeta de destino en cada VM (Master y los 3 Workers)

```powershell
multipass exec spark-lab -- sudo mkdir -p /data
multipass exec spark-lab -- sudo chown ubuntu:ubuntu /data
```

(se repite el mismo comando cambiando `spark-lab` por el nombre de cada VM: Master, Worker 1, Worker 2, Worker 3)

#### Paso 2 — Transferir el archivo a cada VM, en la misma ruta

```powershell
multipass transfer "C:\Users\usuario\Downloads\crimenes.csv" spark-lab:/data/crimenes.csv
```

Este comando se ejecuta **desde la computadora física dueña de esa VM** — es decir, cada una de las 4 personas transfiere el archivo hacia su propia VM local, no se puede transferir directamente de una VM a otra con `multipass transfer` (ese comando solo mueve archivos entre el Windows host y su propia VM).

#### Paso 3 — Verificar en cada VM que el archivo llegó bien

```bash
ls -lh /data/crimenes.csv
```

Se debe confirmar que el **tamaño en bytes coincide** en las 4 máquinas — una diferencia de tamaño indicaría una transferencia incompleta o corrupta en alguna VM.

*(Aquí puedes insertar la captura de `ls -lh /data/crimenes.csv` corriendo en las 4 VMs, mostrando el mismo tamaño de archivo)*

> **Nota:** este paso de copiar el archivo a los 3 workers explica, en retrospectiva, uno de los primeros errores del proyecto (documentado al inicio de la práctica): cuando el archivo solo existía en el Master, los workers no podían acceder a su partición y las lecturas se comportaban de forma inconsistente. Copiar el archivo a los 4 nodos con la misma ruta fue, junto con la red bridged, uno de los dos requisitos indispensables para que el cluster funcionara.

### 16.11. Problemas encontrados durante las pruebas y su solución

| Problema observado | Causa | Solución aplicada |
|---|---|---|
| Un worker aparecía como **DEAD** en la Master UI de forma intermitente | Inestabilidad propia del Wi-Fi (latencia variable, microcortes) sumada a timeouts por defecto de Spark, muy cortos para este tipo de red | Se aumentaron los timeouts: `spark.network.timeout=300s`, `spark.executor.heartbeatInterval=60s`, `spark.worker.timeout=180`, `spark.rpc.askTimeout=300s`, `spark.rpc.lookupTimeout=300s` |
| Un executor cargaba mucho más trabajo que los otros | Los recursos de Spark (`executor.cores`, `executor.memory`) nunca se configuraron explícitamente, por lo que el reparto no era uniforme | Se fijaron `spark.executor.cores` y `spark.executor.memory` de forma explícita, según los recursos reales de cada VM |
| Error al transferir el CSV a una VM (`la carpeta /data no existe`) | La carpeta de destino nunca se había creado dentro de esa VM | Se creó la carpeta con `multipass exec ... mkdir -p /data` antes de repetir la transferencia |
| Lectura del CSV muy lenta o inconsistente entre ejecuciones | Doble escaneo del archivo por `inferSchema=true`, agravado por la latencia de Wi-Fi | Se reemplazó `inferSchema` por un `StructType` con el schema definido manualmente |
| Alguna tarea individual mucho más lenta que el resto (straggler) | Variabilidad normal de una red Wi-Fi doméstica | Se activó **speculation** (`spark.speculation=true`) para que Spark relance automáticamente la copia de una tarea rezagada en otro executor |

- 2 Procesos vivos, 1 muerto y 1 sin iniciarse aun:
  
<img width="1600" height="135" alt="image" src="https://github.com/user-attachments/assets/e54e8aa0-6ef6-4d55-9a16-7a9416865181" />

- Todos los procesos vivos:

<img width="1600" height="827" alt="image" src="https://github.com/user-attachments/assets/470ea4f1-1e19-4c6a-923d-4906e791cb40" />


### 16.12. Limitación reconocida del proyecto

Se documenta como limitación conocida que, al usar **Wi-Fi doméstica en vez de una red cableada**, el rendimiento del cluster es notablemente menor al que se obtendría con Ethernet. Aunque la red bridged eliminó la necesidad de redirección de puertos, la latencia inherente del Wi-Fi sigue explicando los tiempos de ejecución más variables y la necesidad de aumentar los timeouts de Spark para tolerar la inestabilidad de la red inalámbrica.

---

## 17. Anexo: Decisión de reducir el dataset de 5 GB a 2 GB para el EDA
 
Durante la fase de Análisis Exploratorio de Datos (EDA) se tomó la decisión de **reemplazar el dataset original de ~5 GB por un dataset distinto de ~2 GB**: se pasó del archivo de eventos de e-commerce (`2019-Oct.csv`) al dataset público de criminalidad **`Crimes_-_2001_to_Present.csv`**, que en su tamaño original ya pesa aproximadamente 2 GB. Esta sección documenta el motivo de este cambio.
 
### 17.1. Motivo del cambio
 
Aunque el cluster ya lograba conectar correctamente sus 4 nodos (1 Master + 3 Workers) y ejecutar lecturas del CSV completo, el procesamiento de los **5 GB** resultaba **demasiado pesado para los recursos reales disponibles**, considerando en conjunto:
 
- Los recursos limitados de cada VM (CPU y memoria repartidos entre 4 máquinas domésticas, no servidores dedicados).
- La latencia y variabilidad de la red **Wi-Fi** utilizada para conectar las 4 computadoras (documentada en la sección 16), que ralentiza cualquier operación que requiera comunicación entre nodos (shuffles, agregaciones, consolidación de resultados).
- Que el objetivo del proyecto es el **EDA** (análisis exploratorio: estadísticas descriptivas, nulos, distribuciones, correlaciones), un tipo de trabajo que no requiere necesariamente 5 GB de datos para obtener conclusiones representativas y con valor analítico.
En otras palabras: **5 GB era técnicamente procesable, pero no práctico** para el hardware y la red disponibles en este laboratorio, generando tiempos de espera excesivos y mayor probabilidad de que algún worker se cayera por timeout durante operaciones largas.
 
### 17.2. Dataset final utilizado
 
En vez de generar una muestra reducida del archivo original mediante código (por ejemplo con `sample()` en Spark), se optó por una solución más simple: **usar directamente un dataset público distinto cuyo tamaño natural ya era de aproximadamente 2 GB**, sin necesidad de recortarlo artificialmente.
 
| Detalle | Valor |
|---|---|
| Nombre del archivo | `Crimes_-_2001_to_Present.csv` |
| Tamaño aproximado | ~2 GB |
| Naturaleza del dataset | Registro histórico de crímenes reportados (Chicago Police Department, 2001–presente) |
 
*(Aquí puedes insertar la captura de la descarga del archivo, mostrando su tamaño y progreso)*
 
### 17.3. Distribución del nuevo archivo a los 4 nodos
 
Al tratarse de un archivo distinto (no una versión recortada del anterior), fue necesario repetir el proceso de distribución descrito en la sección 16.10: para cada una de las 4 máquinas virtuales, se creó la carpeta `/data` y se clonó (copió) el archivo `Crimes_-_2001_to_Present.csv` dentro de ella, de manera que las 4 VMs (Master + 3 Workers) tuvieran una copia idéntica en la misma ruta:
 
```powershell
multipass exec spark-lab -- sudo mkdir -p /data
multipass exec spark-lab -- sudo chown ubuntu:ubuntu /data
multipass transfer "C:\Users\usuario\Downloads\Crimes_-_2001_to_Present.csv" spark-lab:/data/Crimes_-_2001_to_Present.csv
```
 
(este proceso se repite en cada una de las 4 computadoras, apuntando a su propia VM)
 
### 17.4. Resultado del cambio de dataset
 
| | Dataset original (e-commerce) | Dataset usado para el EDA |
|---|---|---|
| Archivo | `2019-Oct.csv` | `Crimes_-_2001_to_Present.csv` |
| Tamaño | ~5 GB | ~2 GB |
| Tiempo de lectura + procesamiento | Alto, con riesgo de timeouts en Wi-Fi | Notablemente menor y más estable |
| Validez para el EDA | — | Dataset completo, público y representativo de un dominio distinto (criminalidad en vez de e-commerce) |
 
*(Aquí puedes insertar la captura de la Spark UI mostrando la lectura del archivo de 2 GB completándose sin quedarse colgada)*
 
### 17.5. Justificación académica
 
Ajustar el volumen de datos de trabajo a los recursos reales disponibles es una práctica válida en un entorno de laboratorio con hardware doméstico. Al optar por un dataset completo de menor tamaño en vez de forzar el procesamiento de uno más pesado, se garantiza que el EDA se pueda ejecutar de forma **completa e íntegra** (sin muestreo ni pérdida de filas), evitando además los cuellos de botella de red identificados en la sección 16.
 
---

## 18. Resultados del EDA ejecutado en el cluster
 
Esta sección documenta los resultados reales obtenidos al ejecutar el Análisis Exploratorio de Datos sobre el dataset `Crimes_-_2001_to_Present.csv`, corriendo en el notebook de Zeppelin (`orube1`) distribuido sobre los 4 nodos del cluster (Master + 3 Workers).
 
### 18.1. Verificación inicial del cluster
 
Antes de trabajar con el dataset, se verificó que el cluster respondiera correctamente con pruebas simples:
 
```scala
%spark
sc.version
```
Resultado: `4.1.2`
 
```scala
%spark
val datos = spark.range(0, 100000000, 1, 12)
println("Particiones: " + datos.rdd.getNumPartitions)
val pares = datos.filter($"id" % 2 === 0)
println("Pares: " + pares.count())
```
Resultado: **12 particiones**, **50,000,000 números pares** sobre 100 millones de registros generados — confirmando que el cluster distribuye y procesa correctamente antes de tocar el CSV real.
 
### 18.2. Carga del dataset
 
```scala
%spark
val RUTA = "/data/crimenes.csv"
val dfRaw = spark.read.option("header", "true").csv(RUTA)
 
println(s"Filas: ${dfRaw.count()} | Columnas: ${dfRaw.columns.length}")
dfRaw.printSchema()
dfRaw.show(5, false)
```
 
**Resultado:**
 
| Métrica | Valor |
|---|---|
| Filas | **8,596,454** |
| Columnas | **22** |
 
Esquema del dataset (`Crimes_-_2001_to_Present.csv`):
 
```text
root
 |-- ID: string
 |-- Case Number: string
 |-- Date: string
 |-- Block: string
 |-- IUCR: string
 |-- Primary Type: string
 |-- Description: string
 |-- Location Description: string
 |-- Arrest: string
 |-- Domestic: string
 |-- Beat: string
 |-- District: string
 |-- Ward: string
 |-- Community Area: string
 |-- FBI Code: string
 |-- X Coordinate: string
 |-- Y Coordinate: string
 |-- Year: string
 |-- Updated On: string
 |-- Latitude: string
 |-- Longitude: string
 |-- Location: string
```
 
<img width="1132" height="92" alt="image" src="https://github.com/user-attachments/assets/390397ec-e060-453b-b65f-391b1725502e" />

 
### 18.3. Top de tipos de crimen (`Primary Type`)
 
```scala
%spark
import org.apache.spark.sql.functions._
 
val topCrimenes = df.groupBy("Primary Type")
  .count()
  .orderBy(desc("count"))
  .limit(15)
 
z.show(topCrimenes)
```
 
| Primary Type | Total |
|---|---|
| THEFT | 1,826,137 |
| BATTERY | 1,566,015 |
| CRIMINAL DAMAGE | 976,604 |
| NARCOTICS | 768,539 |
| ASSAULT | 579,524 |
| OTHER OFFENSE | 537,092 |
| BURGLARY | 455,080 |
| MOTOR VEHICLE THEFT | 444,216 |
| DECEPTIVE PRACTICE | 399,700 |
| ROBBERY | 317,972 |
| CRIMINAL TRESPASS | 230,726 |
| WEAPONS VIOLATION | 128,415 |
| PROSTITUTION | 70,524 |
| OFFENSE INVOLVING CHILDREN | 61,753 |
| PUBLIC PEACE VIOLATION | 55,675 |
 
**Hallazgo:** el hurto (`THEFT`) y las agresiones (`BATTERY`) concentran, por sí solos, más del 39% de los registros del dataset.
 
<img width="1211" height="274" alt="image" src="https://github.com/user-attachments/assets/b2f6bd93-94b4-4619-8088-6bdb436570c3" />

 
### 18.4. Proporción de arrestos (`Arrest`)
 
```scala
%spark
val arrestos = df.groupBy("Arrest").count()
z.show(arrestos)
```
 
| Arrest | Total |
|---|---|
| false | 6,443,946 |
| true | 2,152,508 |
 
**Hallazgo:** solo alrededor del **25%** de los crímenes reportados terminaron en un arresto — la gran mayoría (75%) quedó sin detención registrada.
 
### 18.5. Consultas SQL sobre el dataset (`%sql`)
 
Se registró el DataFrame como vista temporal para poder consultarlo con SQL directamente desde Zeppelin:
 
```scala
%spark
df.createOrReplaceTempView("crimenes")
```
 
**Top 10 lugares donde ocurren los crímenes (`Location Description`):**
 
```sql
%sql
SELECT `Location Description`, COUNT(1) AS Total
FROM crimenes
WHERE `Location Description` IS NOT NULL
GROUP BY `Location Description`
ORDER BY Total DESC
LIMIT 10
```
 
| Location Description | Total |
|---|---|
| STREET | 2,247,906 |
| RESIDENCE | 1,403,968 |
| APARTMENT | 1,035,804 |
| SIDEWALK | 770,362 |
| OTHER | 269,917 |
| PARKING LOT/GARAGE (NON. RESID.) | 202,913 |
| ALLEY | 190,953 |
| SMALL RETAIL STORE | 175,375 |
| SCHOOL, PUBLIC, BUILDING | 146,365 |
| RESTAURANT | 145,359 |
 
**Hallazgo:** la vía pública (`STREET`) y las viviendas (`RESIDENCE`, `APARTMENT`) concentran la mayoría de los incidentes, coherente con que `THEFT` y `BATTERY` sean los tipos de crimen más comunes.
 
<img width="1210" height="274" alt="image" src="https://github.com/user-attachments/assets/4b6c7054-361b-49de-b491-8c9d0635653f" />

 
### 18.6. Evidencia de ejecución distribuida real
 
Las URLs de la Spark UI que Zeppelin adjunta a cada job (visibles en el notebook, ej. `http://172.29.113.169:4040/jobs/job?id=18`) confirman que estas consultas se ejecutaron realmente sobre el cluster distribuido (con la IP bridged de una de las VMs), y no en modo local — validando que toda la arquitectura documentada en las secciones 1 a 17 funcionó de punta a punta para producir estos resultados.
 
---

## 18. Conclusión

Con esta arquitectura, el procesamiento del CSV se reparte entre los executors de las 4 máquinas físicas, en vez de depender de una sola computadora. Solo la máquina Master necesita el notebook de Zeppelin, ya que actúa como punto único de entrada (driver) para todo el cluster; las máquinas Worker únicamente requieren Spark instalado para poder recibir y ejecutar las tareas asignadas. Además, ante las limitaciones reales de hardware y de red (Wi-Fi) del laboratorio, se ajustó el volumen de datos de 5 GB a 2 GB para garantizar que el EDA pudiera completarse de forma estable y en tiempos razonables., el procesamiento del CSV de 5 GB se reparte entre los executors de las 4 máquinas físicas, en vez de depender de una sola computadora. Solo la máquina Master necesita el notebook de Zeppelin, ya que actúa como punto único de entrada (driver) para todo el cluster; las máquinas Worker únicamente requieren Spark instalado para poder recibir y ejecutar las tareas asignadas.
