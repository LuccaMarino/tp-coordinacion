# Informe

## Coordinación entre Sum y Aggregation

Todas las réplicas de Sum consumen de una misma cola de entrada, así que los registros de un cliente se reparten entre ellas sin que ninguna sepa qué recibieron las otras réplicas. Por lo tanto, el mensaje de fin de un cliente solamente lo recibe una sola de ellas, pero cada una debe saber que el cliente terminó de mandar datos para enviar los totales locales en cada Sum de este cliente a los Aggregation.

Para resolver esto, los Sum se coordinan entre sí a través de un exchange de control. La réplica que recibe el aviso de fin no procesa nada por su cuenta, sino que lo retransmite por ese exchange, donde cada réplica tiene su propia cola. Cada Sum recibe este aviso desde el exchange (incluido la réplica que lo retransmitió), y ahí cada uno envía los totales que acumuló para ese cliente.

Cada réplica de Sum reparte sus totales entre las Aggregation según el nombre de la fruta: se calcula un hash deterministico (`crc32`) del nombre y se toma el resto por la cantidad de Aggregation configuradas. El cálculo da el mismo resultado en todas las réplicas, así que una fruta siempre termina en la misma Aggregation, sin importar qué Sum la haya procesado. De esa forma el total de cada fruta queda completo en un solo lugar y no se duplica trabajo.

Después de enviar sus totales, cada Sum manda un aviso de fin a todos las
Aggregation, incluso a los que no le tocó ninguna fruta de ese cliente, y aunque esa réplica no haya procesado ni un registro suyo. Esto es porque cada Aggregation cierra el cliente cuando recibió un aviso por cada Sum de ese cliente.

Una vez que un Aggregation recibió todos los avisos de un cliente, calcula su top parcial de las frutas que le corresponden y se lo envía al Join. Aqui se repite el mismo esquema que entre Sums-Aggregations: el Join espera un top parcial del cliente por cada Aggregation antes de armar el top final. Como ninguna fruta quedó partida entre dos Aggregation, el Join no necesita volver a sumar nada y solo tiene que juntar los tops parciales y quedarse con los mayores.

Todo esto es posible a través de un identificador de cliente que el gateway (message_handler) le asigna a cada conexión y que viaja en todos los mensajes internos, entonces cada controlador sabe a quien corresponde cada registro, eof y top.

## Escalabilidad respecto a los clientes

Cada conexión recibe su propio identificador, y todos los controles guardan el estado separado por cliente: Sum acumula totales por cliente, Aggregation lleva un contador de avisos por cliente y acumula un top parcial por cliente, y el Join junta los tops parciales por cliente. 

Cada consulta se resuelve de forma independiente. Los mensajes de distintos clientes pueden llegar mezclados entre sí, y gracias al identificador esto no generá ningun problema. Cuando un cliente termina, todos los controladores saben a qué cliente corresponde cada dato, y en conjunto le pueden entregar el resultado. 

Al terminar un cliente, su estado se borra, así que la memoria usada depende de la cantidad de clientes activos en ese momento.

Finalmente, el Join incluye el identificador en el top final, así el gateway sabe a qué conexión devolverle cada resultado.

## Escalabilidad respecto a grandes volúmenes de datos

Del lado del sistema, los registros se procesan de a uno a medida que llegan y cada etapa reduce la cantidad de datos antes de pasarlos a la siguiente:

- Sum recibe muchos registros pero guarda un solo total por fruta. No envia un mensaje por registro al Aggregtaion, sino uno por fruta distinta.
- Aggregation no manda todas sus frutas al Join, sino únicamente las frutas que entran en su top parcial (según TOP_SIZE), ya que una fruta que este en el top global no puede quedar afuera del top parcial en su Aggregation.

Esto significa que duplicar el tamaño de los archivos de entrada no duplica el tráfico interno. El volumen crece solo entre el cliente y las réplicas de Sum, y a partir de ahí depende de la cantidad de frutas distintas, no de la cantidad de registros.

## Escalabilidad respecto a la cantidad de controles

Respecto a los Sum, como todas las réplicas consumen de la misma cola de entrada, el reparto de los registros es automático y todas reciben trabajo.

En cuanto a los Aggregation, las frutas se distribuyen entre todas las réplicas que haya según el hash y AGGREGATION_AMOUNT. Es importante tener en cuenta que si hay menos tipos de frutas que réplicas (poco probable, a menos que AGGREGATION_AMOUNT sea grande o hayan pocos tipos de frutas), alguna réplica podría quedarse sin trabajo, simplemente porque no hay tipos de frutas suficientes para repartir.

El costo de coordinarlas se mantiene bajo. Por cada cliente que termina se envía un único mensaje al exchange de control, sin importar cuántas réplicas de Sum haya, y los avisos de fin hacia Aggregation son uno por cada par Sum-Aggregation, que es el mínimo necesario para que cada Aggregation sepa que recibió todo. Nunca se transmiten datos redundantes: cada fruta viaja a una sola Aggregation y cada top parcial al unico Join.