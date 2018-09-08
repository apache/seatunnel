event = new java.util.HashMap();
you = value.getString("name");
age = value.getLong("msg");
System.out.println(value);
if(age > 10){
event.put("a",you);
}
return event;