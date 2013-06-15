Yet Another Go NATS Client
==========================

(or: You Ain't Gonna Need Another TIBCO System)

A simple client for NATS written in Go.

Basic usage:

```go
client, err := yagnats.Dial("127.0.0.1:4222")
if err != nil {
  panic("Can't contact it, dude.")
}

err = client.Connect("user", "pass")
if err != nil {
  panic("Wrong auth, numbnuts.")
}

client.Subscribe("some.subject", func(msg *Message) {
  fmt.Printf("Got message: %s\n", msg.Payload)
})

client.Publish("some.subject", "Sup son?")
```
