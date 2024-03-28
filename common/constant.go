package common

const MsgAuthStatus = 3
const MsgIm = 4
const MsgAck = 5
const MsgGroupNotification = 7
const MsgGroupIm = 8
const MsgPing = 13
const MsgPong = 14
const MsgAuthToken = 15
const MsgRt = 17
const MsgEnterRoom = 18
const MsgLeaveRoom = 19
const MsgRoomIm = 20
const MsgSystem = 21
const MsgUnreadCount = 22
const MsgCustomerService = 23
const MsgCustomer = 24        //顾客->客服
const MsgCustomerSupport = 25 //客服->顾客
const MsgSync = 26            //客户端->服务端
const MsgSyncBegin = 27       //服务端->客服端
const MsgSyncEnd = 28
const MsgSyncNotify = 29 //通知客户端有新消息
const MsgSyncGroup = 30  //同步超级群消息
const MsgSyncGroupBegin = 31
const MsgSyncGroupEnd = 32
const MsgSyncGroupNotify = 33
const MsgSyncKey = 34
const MsgGroupSyncKey = 35
const MsgNotification = 36
const MsgVoipControl = 64

const MessageFlagText = 0x01         //文本消息
const MessageFlagUnpersistent = 0x02 //消息不持久化
const MessageFlagGroup = 0x04
const MessageFlagSelf = 0x08 //离线消息由当前登录的用户在当前设备发出

const PlatformIos = 1
const PlatformAndroid = 2
const PlatformWeb = 3

const DefaultVersion = 1
const MsgHeaderSize = 12
