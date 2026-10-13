package testsupport

import "gorm.io/gorm"

// MigrateFunc 是跑迁移的函数签名。testsupport 不能直接 import
// internal/manager/repo 去调用 repo.NewMigrator(db).Migrate(...)：repo
// 自己的 _test.go 文件需要 import testsupport（这正是本包存在的原因），
// 如果 testsupport 反过来 import repo，会在编译 repo 的测试二进制时形成
// import cycle——Go 明确禁止这种环（`go vet`/`go test` 直接报错拒绝编译，
// 不是运行时才发现的问题），实测验证过。所以迁移逻辑本身留在调用方，
// testsupport 只负责 schema 的生命周期（建库、复位、清理），通过依赖
// 注入解耦，不持有对具体迁移实现的编译期依赖。
type MigrateFunc func(db *gorm.DB) error

// migrator 是当前注册的迁移函数，SharedDB/FreshDB 内部调用它。
var migrator MigrateFunc

// SetMigrator 注册迁移函数，调用方在自己包的 TestMain 里、m.Run() 之前
// 调用一次：
//
//	func TestMain(m *testing.M) {
//		testsupport.SetMigrator(func(db *gorm.DB) error {
//			return repo.NewMigrator(db).Migrate(context.Background(), time.Now())
//		})
//		code := m.Run()
//		testsupport.TeardownShared()
//		os.Exit(code)
//	}
//
// 不调用就直接用 SharedDB/FreshDB 会在第一次需要迁移时 t.Fatalf，报错信息
// 直接点出"没有注册迁移函数"，不会是一个含糊的空指针崩溃。
func SetMigrator(fn MigrateFunc) { migrator = fn }
