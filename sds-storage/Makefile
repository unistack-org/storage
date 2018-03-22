FLAGS_DEFAULT := 'proxy_sheepdog backend_filesystem transport_tcp hash_xxhash'
FLAGS_MINIMAL := 'proxy_sheepdog backend_filesystem transport_tcp hash_xxhash'

all:
	go build -tags $(FLAGS_DEFAULT)

release:
	go build -tags $(FLAGS_DEFAULT) --ldflags '-s -w'

minimal:
	go build -tags $(FLAGS_MINIMAL)

test:
	#sudo qemu-nbd -f raw --cache=none --aio=threads --discard=unmap --detect-zeroes=unmap -c /dev/nbd0 sheepdog:test
	#fio
	#qemu-img create -f raw sheepdog:127.0.0.1:7000:test 5G
	#qemu-system-x86_64 -machine q35 -cpu kvm64 -smp 2 -accel kvm -m 512M -vnc 0.0.0.0:10 -device virtio-scsi-pci,id=scsi0,iothread=iothread0 -drive aio=threads,rerror=stop,werror=stop,if=none,format=raw,id=drive-scsi-disk0,cache=none,file=sheepdog:test,discard=unmap,detect-zeroes=off  -device scsi-hd,bus=scsi0.0,drive=drive-scsi-disk0,id=device-scsi-disk0 -object iothread,id=iothread0
