### function

Desensitize the user's mobile phone number to enhance data integrity


### How to use

```
transform {
  EncryptedPhone {
  	#You can specify or do not specify. If you do not specify, all fields will be processed
    fields = ["msg", "name"]
  }
}
```

### How to achieve

Perform regular matching on the specified field and replace the middle number of the suspected mobile phone number