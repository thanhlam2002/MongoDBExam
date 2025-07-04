from pymongo import MongoClient

# Kết nối đến MongoDB local
client = MongoClient("mongodb://localhost:27017")

# Chọn database và collection
db = client["eShop"]
order_collection = db["OrderCollection"]

# Xóa toàn bộ dữ liệu cũ trong collection
order_collection.delete_many({})

# Câu 2: Insert dữ liệu mẫu vào collection
orders = [
    {
        "orderid": 1,
        "products": [
            {"product_id": "quanau", "product_name": "quan au", "size": "XL", "price": 10, "quantity": 1},
            {"product_id": "somi", "product_name": "ao so mi", "size": "XL", "price": 10.5, "quantity": 2}
        ],
        "total_amount": 31,
        "delivery_address": "Hanoi"
    },
    {
        "orderid": 2,
        "products": [
            {"product_id": "somi", "product_name": "ao so mi", "size": "L", "price": 9.5, "quantity": 1},
            {"product_id": "jean", "product_name": "quan jean", "size": "M", "price": 20, "quantity": 2}
        ],
        "total_amount": 49.5,
        "delivery_address": "HCMC"
    }
]

order_collection.insert_many(orders)

# Đọc dữ liệu vừa insert để hiển thị Câu 2
print("\nCâu 2: Dữ liệu order hiện có sau khi insert:")
orders = order_collection.find()
for order in orders:
    print(order)

# Câu 3: Cập nhật delivery_address theo orderid = 1
update_result = order_collection.update_one(
    {"orderid": 1},
    {"$set": {"delivery_address": "Hai Phong"}}
)
print(f"\nCâu 3: Cập nhật địa chỉ đơn hàng orderid=1, số bản ghi ảnh hưởng: {update_result.modified_count}")

# Câu 4: Xóa một đơn hàng theo orderid = 2
delete_result = order_collection.delete_one({"orderid": 2})
print(f"\nCâu 4: Xóa đơn hàng orderid=2, số bản ghi bị xóa: {delete_result.deleted_count}")

# Câu 5: Hiển thị tất cả đơn hàng dưới dạng bảng với dấu phân cách "|"
print("\nCâu 5: Danh sách order theo bảng:")
orders = order_collection.find()
for order in orders:
    print(f"\nOrder ID: {order['orderid']} | Delivery: {order['delivery_address']}")
    print("+----+----------------+--------+----------+--------+")
    print("| No | Product name   | Price  | Quantity | Total  |")
    print("+----+----------------+--------+----------+--------+")
    for idx, p in enumerate(order['products'], start=1):
        total = p['price'] * p['quantity']
        print(f"| {str(idx).ljust(2)} | {p['product_name'].ljust(14)} | {str(p['price']).ljust(6)} | {str(p['quantity']).ljust(8)} | {str(total).ljust(6)} |")
    print("+----+----------------+--------+----------+--------+")

# Câu 6: Tính tổng total_amount của tất cả đơn hàng
pipeline_total_amount = [
    {"$group": {"_id": None, "total_amount": {"$sum": "$total_amount"}}}
]
result_total_amount = list(order_collection.aggregate(pipeline_total_amount))

if result_total_amount:
    print(f"\nCâu 6: Tổng số tiền của tất cả đơn hàng: {result_total_amount[0]['total_amount']}")
else:
    print("\nCâu 6: Không có đơn hàng nào.")

# Câu 7: Đếm số lượng product_id = 'somi' trong tất cả đơn hàng
pipeline_count_somi = [
    {"$unwind": "$products"},
    {"$match": {"products.product_id": "somi"}},
    {"$count": "total_somi"}
]
result_count_somi = list(order_collection.aggregate(pipeline_count_somi))

if result_count_somi:
    print(f"\nCâu 7: Tổng số sản phẩm 'somi': {result_count_somi[0]['total_somi']}")
else:
    print("\nCâu 7: Không có sản phẩm 'somi' nào.")

print("\n===== XONG =====")

