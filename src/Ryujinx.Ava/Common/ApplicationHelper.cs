using Avalonia.Controls;
using Avalonia.Controls.Notifications;
using Avalonia.Threading;
using LibHac;
using LibHac.Account;
using LibHac.Common;
using LibHac.Fs;
using LibHac.Fs.Fsa;
using LibHac.Fs.Shim;
using LibHac.FsSystem;
using LibHac.Ns;
using LibHac.Tools.Fs;
using LibHac.Tools.FsSystem;
using LibHac.Tools.FsSystem.NcaUtils;
using Ryujinx.Ava.Common.Locale;
using Ryujinx.Ava.UI.Controls;
using Ryujinx.Ava.UI.Helpers;
using Ryujinx.Ava.UI.Windows;
using Ryujinx.Common.Logging;
using Ryujinx.Common.Configuration;
using Ryujinx.HLE.FileSystem;
using Ryujinx.HLE.HOS;
using Ryujinx.HLE.HOS.Services.Account.Acc;
using Ryujinx.Ui.App.Common;
using Ryujinx.Ui.Common.Helper;
using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Path = System.IO.Path;
using System.Globalization;
using System.IO.Compression;
using System.Collections.Generic;

namespace Ryujinx.Ava.Common
{
    internal static class ApplicationHelper
    {
        private static HorizonClient _horizonClient;
        private static AccountManager _accountManager;
        private static VirtualFileSystem _virtualFileSystem;
        private static StyleableWindow _owner;

        public static void Initialize(VirtualFileSystem virtualFileSystem, AccountManager accountManager, HorizonClient horizonClient, StyleableWindow owner)
        {
            _owner = owner;
            _virtualFileSystem = virtualFileSystem;
            _horizonClient = horizonClient;
            _accountManager = accountManager;
        }
        public static async Task<bool> BackupSaveDir(IEnumerable<UserProfile> users, string titleId, string titleName, bool single = true)
        {
            // TODO: Avoid copying all files to temp directory and then zipping
            //       Rather, open a zip file and chain writes to it, then copy that zip to final dest

            // Need to backup: SaveDataType.Bcat, SaveDataType.Device (for UserID default)
            //                 SaveDataType.Account (for all User IDs)
            //                 Metadata
            //                 Maybe ? User Profiles (in order to handle mapping on import)

            // Backup structure:
            // Root
            // |_ {TitleId}
            //   |_ Ryu
            //   |_ Device
            //   |_ BCAT
            //   |_ User
            //     |_ 0
            //     |_ 1
            // |_ {TitleId}
            //   |_ Ryu
            //   |_ Device
            //   |_ BCAT
            //   |_ User
            //     |_ 0
            //     |_ 1

            if (!ulong.TryParse(titleId, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out ulong titleIdNumber))
            {
                Logger.Error?.Print(LogClass.Application, $"Invalid titleId {titleId}");
                return false;
            }

            
            string backupRoot = Path.Combine(AppDataManager.BaseDirPath, "backup");
            string titleBackupRoot = Path.Combine(backupRoot, titleId);

            List<string> createdBackupPaths = new List<string>();
            
            // backup users saves
            foreach (var user in users) {
                var userId = new LibHac.Fs.UserId((ulong)user.UserId.High, (ulong)user.UserId.Low);
                var createdPath = BackupUserSaveDir(userId, titleId, titleIdNumber, titleBackupRoot);
                if (createdPath != "") {
                    createdBackupPaths.Add(createdPath);
                }
            }

            // backup device save
            var deviceBackupPath = BackupDeviceSaveDir(titleId, titleIdNumber, titleBackupRoot);
            if (deviceBackupPath != "") {
                createdBackupPaths.Add(deviceBackupPath);
            }
            // backup bcat save
            var bcatBackupPath = BackupBCATSaveDir(titleId, titleIdNumber, titleBackupRoot);
            if (bcatBackupPath != "") {
                createdBackupPaths.Add(bcatBackupPath);
            }

            // backup ryujinx data
            var ryuBackupPath = BackupRyujinxData(titleId, titleBackupRoot);
            if (ryuBackupPath != "") {
                createdBackupPaths.Add(ryuBackupPath);
            }

            if (createdBackupPaths.Count > 0) {
                Logger.Info?.Print(LogClass.Application, $"Successfully backed up save data folder for {titleName} [{titleId}]");

                 // if not part of a full backup, output the backup and then cleanup after ourselves
                if (single) {
                    bool didOutputSucceed = await OutputBackupZip(titleId);
                    Directory.Delete(backupRoot, true);

                    return didOutputSucceed;
                }

                return true;
            } else {
                Logger.Error?.Print(LogClass.Application, $"Failed to backup save data folder for {titleName} [{titleId}].");

                // cleanup remains
                foreach (var path in createdBackupPaths) {
                    if (Directory.Exists(path)) {
                        Directory.Delete(path, true);
                    }
                }
            }

            return false;
        }

        private static string BackupRyujinxData(string titleId, string titleBackupRoot) {
            string metadataPath = Path.Combine(AppDataManager.GamesDirPath, titleId, "gui"); // only save gui info bc update/dlc/caches are system dependent
            string outputPath = Path.Combine(titleBackupRoot, "Ryu", "gui");
            return CopyDirectory(metadataPath, outputPath) ? outputPath : "";
        }

        // fetch the user save data for userId and titleId and backs it up to /backup dir 
        private static string BackupUserSaveDir(LibHac.Fs.UserId userId, string titleId, ulong titleIdNumber, string titleBackupRoot) {
            var saveDataFilter = SaveDataFilter.Make(titleIdNumber, SaveDataType.Account, userId, saveDataId: default, index: default);
            string outputPath = Path.Combine(titleBackupRoot, $"User/{userId.ToString()}");
            return BackupSaveData(saveDataFilter, titleId, outputPath);
        }

        // fetch the device save data for titleId and backs it up to /backup dir 
        private static string BackupDeviceSaveDir(string titleId, ulong titleIdNumber, string titleBackupRoot) {
            var saveDataFilter = SaveDataFilter.Make(titleIdNumber, SaveDataType.Device, userId: default, saveDataId: default, index: default);
            string outputPath = Path.Combine(titleBackupRoot, "Device");
            return BackupSaveData(saveDataFilter, titleId, outputPath);
        }

        // fetch the device save data for titleId and backs it up to /backup dir 
        private static string BackupBCATSaveDir(string titleId, ulong titleIdNumber, string titleBackupRoot) {
            var saveDataFilter = SaveDataFilter.Make(titleIdNumber, SaveDataType.Bcat, userId: default, saveDataId: default, index: default);
            string outputPath = Path.Combine(titleBackupRoot, "BCAT");
            return BackupSaveData(saveDataFilter, titleId, outputPath);
        }

        // fetch the save data based on the filter and back it up to /backup dir
        private static string BackupSaveData(SaveDataFilter saveDataFilter, string titleId, string outputPath) {
            ulong saveDataId = default;

            Result result = _horizonClient.Fs.FindSaveDataWithFilter(out SaveDataInfo saveDataInfo, SaveDataSpaceId.User, in saveDataFilter);
            
            if (result.IsSuccess()) {
                saveDataId = saveDataInfo.SaveDataId;
            } else {
                Logger.Warning?.Print(LogClass.Application, $"No save data was found for [{titleId}]. Skipping backup.");
                return "";
            }

            string saveRootPath = Path.Combine(_virtualFileSystem.GetNandPath(), $"user/save/{saveDataId:x16}");

            Logger.Info?.Print(LogClass.Application, $"Want to backup {saveRootPath} to {outputPath}");

            if (Directory.Exists(outputPath))
            {
                Logger.Info?.Print(LogClass.Application, $"Backup already exists at {outputPath}, deleting.");
                Directory.Delete(outputPath, true);
            }

            bool didCopySucceed = CopyDirectory(saveRootPath, outputPath);

            return didCopySucceed ? outputPath : "";
        }

        private static async Task<bool> OutputBackupZip(string titleId = "")
        {
            // resolve paths
            string backupRoot = Path.Combine(AppDataManager.BaseDirPath, "backup");
            string backupPath = backupRoot;
            string dateString = DateTime.UtcNow.ToString("yyyy_MM_dd");
            string defaultFilename = $"Ryujinx_Backup_{dateString}.zip";
            if (titleId != "") {
                backupPath = Path.Combine(backupPath, titleId);
                defaultFilename = $"Ryujinx_Backup_{titleId}_{dateString}.zip";
            }
            
            // prepare save dialog
            FileDialogFilter zipFilter = new FileDialogFilter();
            zipFilter.Extensions = new List<string> { ".zip" };
            zipFilter.Name = "ZIP File";
            var filters = new List<FileDialogFilter> { zipFilter };
            
            string zipPath = await ContentDialogHelper.ShowSaveFileDialog("Save backup as...", defaultFilename, filters, ".zip");

            // if user chose an output location, dump the data to it
            if (zipPath != null && zipPath != "") {
                if (File.Exists(zipPath)) {
                    File.Delete(zipPath);
                }
                ZipFile.CreateFromDirectory(backupPath, zipPath);
            }

            // return true if the output file was successfully created
            return File.Exists(zipPath);
        }

        // Returns `true` if entire directory contents were successfully copied
        private static bool CopyDirectory(string sourcePath, string destinationPath)
        {
            // TODO: Guard against not having enough space at destination 
            bool didError = false;

            if (!Directory.Exists(destinationPath))
            {
                Directory.CreateDirectory(destinationPath);
            }

            string[] files = Directory.GetFiles(sourcePath);
            foreach (string file in files)
            {
                try {
                    string name = Path.GetFileName(file);
                    string dest = Path.Combine(destinationPath, name);
                    File.Copy(file, dest);
                } catch {
                    Logger.Error?.Print(LogClass.Application, $"Unable to backup file {file} to {destinationPath}");
                    didError = true;
                }
            }

            string[] folders = Directory.GetDirectories(sourcePath);
            foreach (string folder in folders)
            {
                try {
                    string name = Path.GetFileName(folder);
                    string dest = Path.Combine(destinationPath, name);
                    bool didSubCopySucceed = CopyDirectory(folder, dest);
                    didError = didError || !didSubCopySucceed;
                } catch {
                    Logger.Error?.Print(LogClass.Application, $"Unable to backup directory {folder} to {destinationPath}");
                    didError = true;
                }
            }

            return !didError;
        }

        private static bool TryFindSaveData(string titleName, ulong titleId, BlitStruct<ApplicationControlProperty> controlHolder, in SaveDataFilter filter, out ulong saveDataId)
        {
            saveDataId = default;

            Result result = _horizonClient.Fs.FindSaveDataWithFilter(out SaveDataInfo saveDataInfo, SaveDataSpaceId.User, in filter);
            if (ResultFs.TargetNotFound.Includes(result))
            {
                ref ApplicationControlProperty control = ref controlHolder.Value;

                Logger.Info?.Print(LogClass.Application, $"Creating save directory for Title: {titleName} [{titleId:x16}]");

                if (Utilities.IsZeros(controlHolder.ByteSpan))
                {
                    // If the current application doesn't have a loaded control property, create a dummy one
                    // and set the savedata sizes so a user savedata will be created.
                    control = ref new BlitStruct<ApplicationControlProperty>(1).Value;

                    // The set sizes don't actually matter as long as they're non-zero because we use directory savedata.
                    control.UserAccountSaveDataSize = 0x4000;
                    control.UserAccountSaveDataJournalSize = 0x4000;

                    Logger.Warning?.Print(LogClass.Application, "No control file was found for this game. Using a dummy one instead. This may cause inaccuracies in some games.");
                }

                Uid user = new((ulong)_accountManager.LastOpenedUser.UserId.High, (ulong)_accountManager.LastOpenedUser.UserId.Low);

                result = _horizonClient.Fs.EnsureApplicationSaveData(out _, new LibHac.Ncm.ApplicationId(titleId), in control, in user);
                if (result.IsFailure())
                {
                    Dispatcher.UIThread.InvokeAsync(async () =>
                    {
                        await ContentDialogHelper.CreateErrorDialog(LocaleManager.Instance.UpdateAndGetDynamicValue(LocaleKeys.DialogMessageCreateSaveErrorMessage, result.ToStringWithName()));
                    });

                    return false;
                }

                // Try to find the savedata again after creating it
                result = _horizonClient.Fs.FindSaveDataWithFilter(out saveDataInfo, SaveDataSpaceId.User, in filter);
            }

            if (result.IsSuccess())
            {
                saveDataId = saveDataInfo.SaveDataId;

                return true;
            }

            Dispatcher.UIThread.InvokeAsync(async () =>
            {
                await ContentDialogHelper.CreateErrorDialog(LocaleManager.Instance.UpdateAndGetDynamicValue(LocaleKeys.DialogMessageFindSaveErrorMessage, result.ToStringWithName()));
            });

            return false;
        }

        public static void OpenSaveDir(in SaveDataFilter saveDataFilter, ulong titleId, BlitStruct<ApplicationControlProperty> controlData, string titleName)
        {
            if (!TryFindSaveData(titleName, titleId, controlData, in saveDataFilter, out ulong saveDataId))
            {
                return;
            }

            OpenSaveDir(saveDataId);
        }

        public static void OpenSaveDir(ulong saveDataId)
        {
            string saveRootPath = Path.Combine(_virtualFileSystem.GetNandPath(), $"user/save/{saveDataId:x16}");

            if (!Directory.Exists(saveRootPath))
            {
                // Inconsistent state. Create the directory
                Directory.CreateDirectory(saveRootPath);
            }

            string committedPath = Path.Combine(saveRootPath, "0");
            string workingPath = Path.Combine(saveRootPath, "1");

            // If the committed directory exists, that path will be loaded the next time the savedata is mounted
            if (Directory.Exists(committedPath))
            {
                OpenHelper.OpenFolder(committedPath);
            }
            else
            {
                // If the working directory exists and the committed directory doesn't,
                // the working directory will be loaded the next time the savedata is mounted
                if (!Directory.Exists(workingPath))
                {
                    Directory.CreateDirectory(workingPath);
                }

                OpenHelper.OpenFolder(workingPath);
            }
        }

        public static async Task ExtractSection(NcaSectionType ncaSectionType, string titleFilePath, string titleName, int programIndex = 0)
        {
            OpenFolderDialog folderDialog = new()
            {
                Title = LocaleManager.Instance[LocaleKeys.FolderDialogExtractTitle]
            };

            string destination       = await folderDialog.ShowAsync(_owner);
            var    cancellationToken = new CancellationTokenSource();

            UpdateWaitWindow waitingDialog = new(
                LocaleManager.Instance[LocaleKeys.DialogNcaExtractionTitle],
                LocaleManager.Instance.UpdateAndGetDynamicValue(LocaleKeys.DialogNcaExtractionMessage, ncaSectionType, Path.GetFileName(titleFilePath)),
                cancellationToken);

            if (!string.IsNullOrWhiteSpace(destination))
            {
                Thread extractorThread = new(() =>
                {
                    Dispatcher.UIThread.Post(waitingDialog.Show);

                    using FileStream file = new(titleFilePath, FileMode.Open, FileAccess.Read);

                    Nca mainNca  = null;
                    Nca patchNca = null;

                    string extension = Path.GetExtension(titleFilePath).ToLower();
                    if (extension == ".nsp" || extension == ".pfs0" || extension == ".xci")
                    {
                        PartitionFileSystem pfs;

                        if (extension == ".xci")
                        {
                            pfs = new Xci(_virtualFileSystem.KeySet, file.AsStorage()).OpenPartition(XciPartitionType.Secure);
                        }
                        else
                        {
                            pfs = new PartitionFileSystem(file.AsStorage());
                        }

                        foreach (DirectoryEntryEx fileEntry in pfs.EnumerateEntries("/", "*.nca"))
                        {
                            using var ncaFile = new UniqueRef<IFile>();

                            pfs.OpenFile(ref ncaFile.Ref, fileEntry.FullPath.ToU8Span(), OpenMode.Read).ThrowIfFailure();

                            Nca nca = new(_virtualFileSystem.KeySet, ncaFile.Get.AsStorage());
                            if (nca.Header.ContentType == NcaContentType.Program)
                            {
                                int dataIndex = Nca.GetSectionIndexFromType(NcaSectionType.Data, NcaContentType.Program);
                                if (nca.SectionExists(NcaSectionType.Data) && nca.Header.GetFsHeader(dataIndex).IsPatchSection())
                                {
                                    patchNca = nca;
                                }
                                else
                                {
                                    mainNca = nca;
                                }
                            }
                        }
                    }
                    else if (extension == ".nca")
                    {
                        mainNca = new Nca(_virtualFileSystem.KeySet, file.AsStorage());
                    }

                    if (mainNca == null)
                    {
                        Logger.Error?.Print(LogClass.Application, "Extraction failure. The main NCA was not present in the selected file");

                        Dispatcher.UIThread.InvokeAsync(async () =>
                        {
                            waitingDialog.Close();

                            await ContentDialogHelper.CreateErrorDialog(LocaleManager.Instance[LocaleKeys.DialogNcaExtractionMainNcaNotFoundErrorMessage]);
                        });

                        return;
                    }

                    (Nca updatePatchNca, _) = ApplicationLibrary.GetGameUpdateData(_virtualFileSystem, mainNca.Header.TitleId.ToString("x16"), programIndex, out _);
                    if (updatePatchNca != null)
                    {
                        patchNca = updatePatchNca;
                    }

                    int index = Nca.GetSectionIndexFromType(ncaSectionType, mainNca.Header.ContentType);

                    try
                    {
                        bool sectionExistsInPatch = false;
                        if (patchNca != null)
                        {
                            sectionExistsInPatch = patchNca.CanOpenSection(index);
                        }

                        IFileSystem ncaFileSystem = sectionExistsInPatch ? mainNca.OpenFileSystemWithPatch(patchNca, index, IntegrityCheckLevel.ErrorOnInvalid)
                                                                         : mainNca.OpenFileSystem(index, IntegrityCheckLevel.ErrorOnInvalid);

                        FileSystemClient fsClient = _horizonClient.Fs;

                        string source = DateTime.Now.ToFileTime().ToString()[10..];
                        string output = DateTime.Now.ToFileTime().ToString()[10..];

                        using var uniqueSourceFs = new UniqueRef<IFileSystem>(ncaFileSystem);
                        using var uniqueOutputFs = new UniqueRef<IFileSystem>(new LocalFileSystem(destination));

                        fsClient.Register(source.ToU8Span(), ref uniqueSourceFs.Ref);
                        fsClient.Register(output.ToU8Span(), ref uniqueOutputFs.Ref);

                        (Result? resultCode, bool canceled) = CopyDirectory(fsClient, $"{source}:/", $"{output}:/", cancellationToken.Token);

                        if (!canceled)
                        {
                            if (resultCode.Value.IsFailure())
                            {
                                Logger.Error?.Print(LogClass.Application, $"LibHac returned error code: {resultCode.Value.ErrorCode}");

                                Dispatcher.UIThread.InvokeAsync(async () =>
                                {
                                    waitingDialog.Close();

                                    await ContentDialogHelper.CreateErrorDialog(LocaleManager.Instance[LocaleKeys.DialogNcaExtractionCheckLogErrorMessage]);
                                });
                            }
                            else if (resultCode.Value.IsSuccess())
                            {
                                Dispatcher.UIThread.Post(waitingDialog.Close);

                                NotificationHelper.Show(
                                    LocaleManager.Instance[LocaleKeys.DialogNcaExtractionTitle],
                                    $"{titleName}\n\n{LocaleManager.Instance[LocaleKeys.DialogNcaExtractionSuccessMessage]}",
                                    NotificationType.Information);
                            }
                        }

                        fsClient.Unmount(source.ToU8Span());
                        fsClient.Unmount(output.ToU8Span());
                    }
                    catch (ArgumentException ex)
                    {
                        Logger.Error?.Print(LogClass.Application, $"{ex.Message}");

                        Dispatcher.UIThread.InvokeAsync(async () =>
                        {
                            waitingDialog.Close();

                            await ContentDialogHelper.CreateErrorDialog(ex.Message);
                        });
                    }
                });

                extractorThread.Name = "GUI.NcaSectionExtractorThread";
                extractorThread.IsBackground = true;
                extractorThread.Start();
            }
        }

        public static (Result? result, bool canceled) CopyDirectory(FileSystemClient fs, string sourcePath, string destPath, CancellationToken token)
        {
            Result rc = fs.OpenDirectory(out DirectoryHandle sourceHandle, sourcePath.ToU8Span(), OpenDirectoryMode.All);
            if (rc.IsFailure())
            {
                return (rc, false);
            }

            using (sourceHandle)
            {
                foreach (DirectoryEntryEx entry in fs.EnumerateEntries(sourcePath, "*", SearchOptions.Default))
                {
                    if (token.IsCancellationRequested)
                    {
                        return (null, true);
                    }

                    string subSrcPath = PathTools.Normalize(PathTools.Combine(sourcePath, entry.Name));
                    string subDstPath = PathTools.Normalize(PathTools.Combine(destPath, entry.Name));

                    if (entry.Type == DirectoryEntryType.Directory)
                    {
                        fs.EnsureDirectoryExists(subDstPath);

                        (Result? result, bool canceled) = CopyDirectory(fs, subSrcPath, subDstPath, token);
                        if (canceled || result.Value.IsFailure())
                        {
                            return (result, canceled);
                        }
                    }

                    if (entry.Type == DirectoryEntryType.File)
                    {
                        fs.CreateOrOverwriteFile(subDstPath, entry.Size);

                        rc = CopyFile(fs, subSrcPath, subDstPath);
                        if (rc.IsFailure())
                        {
                            return (rc, false);
                        }
                    }
                }
            }

            return (Result.Success, false);
        }

        public static Result CopyFile(FileSystemClient fs, string sourcePath, string destPath)
        {
            Result rc = fs.OpenFile(out FileHandle sourceHandle, sourcePath.ToU8Span(), OpenMode.Read);
            if (rc.IsFailure())
            {
                return rc;
            }

            using (sourceHandle)
            {
                rc = fs.OpenFile(out FileHandle destHandle, destPath.ToU8Span(), OpenMode.Write | OpenMode.AllowAppend);
                if (rc.IsFailure())
                {
                    return rc;
                }

                using (destHandle)
                {
                    const int MaxBufferSize = 1024 * 1024;

                    rc = fs.GetFileSize(out long fileSize, sourceHandle);
                    if (rc.IsFailure())
                    {
                        return rc;
                    }

                    int bufferSize = (int)Math.Min(MaxBufferSize, fileSize);

                    byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
                    try
                    {
                        for (long offset = 0; offset < fileSize; offset += bufferSize)
                        {
                            int toRead = (int)Math.Min(fileSize - offset, bufferSize);
                            Span<byte> buf = buffer.AsSpan(0, toRead);

                            rc = fs.ReadFile(out long _, sourceHandle, offset, buf);
                            if (rc.IsFailure())
                            {
                                return rc;
                            }

                            rc = fs.WriteFile(destHandle, offset, buf, WriteOption.None);
                            if (rc.IsFailure())
                            {
                                return rc;
                            }
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }

                    rc = fs.FlushFile(destHandle);
                    if (rc.IsFailure())
                    {
                        return rc;
                    }
                }
            }

            return Result.Success;
        }
    }
}