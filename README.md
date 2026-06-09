# NogTech

Adicionando pastas e sub-pastas no projeto para uma melhor estruturação

Comando para subir o projeto:

Docker compose up -d

Como acessar Web:

http://Seu-IP:8080


### Banco
User: nogtech
Senha: nogtech123
porta: 5433

### Acesso Web
User: admin
Senha: admin
porta: 8080

Estratégia usada para idepotência no qual escolhi:

Para garantir a integridade dos dados e evitar registros duplicados, foi utilizada uma chave natural (`id_transacao`) em conjunto com a operação de **UPSERT**.

O campo `id_transacao` identifica cada transação de forma única no sistema. Com isso, sempre que uma nova carga de dados é processada, o banco de dados verifica se já existe um registro com o mesmo identificador.

A persistência é realizada por meio do comando `INSERT ... ON CONFLICT (id_transacao) DO UPDATE`, que funciona da seguinte forma:

* Se a transação ainda não estiver cadastrada, ela é inserida normalmente.
* Se a transação já existir, seus dados são atualizados em vez de criar um novo registro.

Essa estratégia permite que o processamento seja executado mais de uma vez sem gerar duplicidades, o que é especialmente útil em situações de reprocessamento, recuperação de falhas ou sincronização de dados. Dessa forma, o sistema mantém a consistência das informações e garante que cada transação seja armazenada apenas uma vez.

Alguns problemas que estive tendo:

- Permissão em arquivos, o container subia mas não conseguia copiar resolveu somente aplicando chmod 500 para subir como Root, pois o arquivo estava com permissão para usuarios local