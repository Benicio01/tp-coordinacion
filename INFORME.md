# Informe

## 1. Gateway

A cada cliente que se conecta se le asigna un `client_id` distinto con un contador.
Después todos los mensajes que viajan por adentro llevan ese id adelante:
dato -> `[client_id, fruta, cantidad]`, fin -> `[client_id, total]`.
El `total` es cuántos datos le mandó ese cliente, lo contamos con un `seq`.
A la vuelta se filtra por `client_id` para devolverle a cada cliente su top.

## 2. Sum

Cada Sum guarda un diccionario por cliente, no uno global, para no mezclar
clientes que llegan a la vez.

El problema es que el fin de un cliente le llega a un solo Sum. Si ese Sum avisara y los demás hicieran flush a los Aggregators,
se puede perder un dato que todavía está en camino. Ejemplo: SumA agarra
el EOF pero SumB todavía tiene un mensaje sin procesar.

Se resuelve de la siguente manera:

1. El sum que recibe el EOF no flushea, lo reenvía a un exchange de control
   que le envia una copia a cada Sum, con el total `N` que mandó el gateway.

2. Cada Sum contesta por ese mismo exchange cuántos datos vio él: `[cliente, mi_id, n]`.

3. Cuando un Sum ve que contestaron todos y que la suma de esos `n` da `N`,
   ahí recién suma todo lo suyo, lo parte por fruta y lo manda.
Para hashear se usa `crc32(cliente:fruta) % AGGREGATION_AMOUNT`, así le toca siempre
el mismo Agg a la misma fruta del mismo cliente. Uso cliente+fruta y no
solo fruta para que si hay pocas frutas igual se reparta entre Aggs.
Después le manda fin `[cliente]` a todos los Agg. Sum usa dos hilos
(uno para datos y otro para control) por eso es el único que necesita lock.

## 3. Aggregation

También guarda todo por cliente. Suma lo que le llega de los Sums.
Como cada Sum le manda fin, espera a tener tantos fines como Sums hay
para ese cliente. Ahí arma el top parcial (ordena y corta por TOP_SIZE)
y lo manda al Join como `[cliente, top]`.

## 4. Join

Junta los tops parciales por cliente. Cuando tiene tantos como cantidad de Aggs,
los mezcla: suma por fruta y vuelve a ordenar para el top final.
Ese top va a la cola de resultados y el gateway se lo da al cliente correspondiente.

## 5. Escalabilidad

- Clientes: todo el estado está separado por `client_id` (Sum, Agg y Join
  tienen un diccionario por cliente). Pueden correr varios clientes a la vez
  sin mezclarse. El gateway les asigna el id y a la vuelta filtra por ese id.

- Volumen / controles: la entrada de Sum (`INPUT_QUEUE`) es una cola
  competidora, así que si agrego más Sums se reparten los mensajes solos,
  y luego se coordinan entre ellos para el manejo de EOF.
  La salida de Sum es un exchange `direct` con routing keys
  `aggregation_{ID}`: cada Agg tiene su cola exclusiva y recibe solo su
  partición. El EOF en cambio
  se broadcastea a todos los Agg para que cada uno sepa cuándo cerrar.
  Por eso para escalar solo cambio `SUM_AMOUNT` y `AGGREGATION_AMOUNT`
  en el compose: Sum espera esa cantidad
  de COUNTs, Agg espera esa cantidad de EOFs y Join espera esa cantidad
  de tops parciales. Join y Gateway son únicos por definicion del alcance del trabajo practico.

## 6.Graceful shutdown (Sigterm)

Se agarra con `signal.signal` en el hilo principal.
El handler frena el consumo con `stop_consuming()` y después se cierran
las conexiones con `close()`.

- Agg y Join: tienen un solo consumo, así que el handler frena ese input
  y después cierra input + output.

- Sum: tiene dos consumos (datos en un hilo y control en el principal).
  El handler frena los dos, se espera al hilo de datos con `join(timeout=2)`
  y después se cierra todo: los 2 de consumo más el publisher de control
  y los de datos (uno por cada Agg). El timeout=2 es para no quedarse esperando
  para siempre: si el hilo tarda, se cierra todo igualmente y el container llega
  a salir antes de los 5 segundos del stop.

Se probó con `stop -t 5` en el escenario 5: todos salen con `Exited (0)`
y `Shutdown graceful OK` en los logs.
