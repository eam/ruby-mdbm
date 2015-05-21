#!/usr/bin/env ruby

require 'test/unit'
require 'mdbm'


class TestMdbm < Test::Unit::TestCase
  def fresh_mdbm
    test_mdbm_name = "/tmp/some_mdbm_file.mdbm"
    File.unlink test_mdbm_name rescue true
    db = Mdbm.new(test_mdbm_name, Mdbm::MDBM_O_RDWR|Mdbm::MDBM_O_CREAT, 0644, 0, 0)
    db.store("zzz", "qqq", Mdbm::MDBM_INSERT)
    db.store("\x00starts/ends with null, val is nul\x00", "\x00", Mdbm::MDBM_INSERT)
    db
  end

  def test_open
    assert_raise(RuntimeError) do
      Mdbm.new("", Mdbm::MDBM_O_RDWR|Mdbm::MDBM_O_CREAT, 0644, 0, 0)
    end
    assert_nothing_raised do
       test_mdbm_name = "/tmp/asdkjfaksdj.mdbm"
       File.unlink test_mdbm_name rescue true
       Mdbm.new(test_mdbm_name, Mdbm::MDBM_O_RDWR|Mdbm::MDBM_O_CREAT, 0644, 0, 0)
    end
  end

  def test_store
    db = fresh_mdbm
    assert_nothing_raised do
      db.store("new k/v pair", "data", Mdbm::MDBM_INSERT)
    end
    assert_raise(RuntimeError) do
      # subsequent store with INSERT should raise
      db.store("zzz", "qqq", Mdbm::MDBM_INSERT)
    end
    assert_nothing_raised do
      db.store("zzz", "qqq2", Mdbm::MDBM_REPLACE)
    end
  end

  def test_keys
    db = fresh_mdbm
    assert_equal ["zzz", "\x00starts/ends with null, val is nul\x00"], db.keys
  end

  def test_fetch
    db = fresh_mdbm
    assert_equal "qqq", db.fetch("zzz")
    assert_equal "\x00", db.fetch("\x00starts/ends with null, val is nul\x00")
  end
end
