from demo import user_model, address_model, BaseModel
import datetime
user = BaseModel(
    {'forename':'john','surname':'smith','dob':datetime.date(1971,12,1)},
    instance=user_model
)
user.addresses.append(
    BaseModel(
        {'address': 'address1', 'postcode': 'postcode1'},
        instance=address_model
    )
)
user.addresses.append(
    BaseModel(
        {'address': 'address2', 'postcode': 'postcode2'},
        instance=address_model
    )
)
user_model.add(user)
user_model.list()
address_model.list()

user = list(user_model.find())[0]
del user.addresses[0]
user.save()
user = list(user_model.find())[0]
print("-", user.addresses)

address = list(address_model.find())[0]
print(type(address), address._instance, address._dirty)
#address._dirty = True
user.addresses.append(address)
print("+", user.addresses)
user.save()
print("=", user.addresses)

user = list(user_model.find())[0]
print("/", user.addresses)
user.addresses[0].postcode="GB"
user.save()
user = list(user_model.find())[0]
print("~", user.addresses)

#print(">", user.addresses)
#user.save()
#print("+", user.addresses)
