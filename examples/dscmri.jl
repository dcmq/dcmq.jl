using Pkg
Pkg.activate(".")
using Revise
using DCMQ
using DICOM
using NIfTI
using DSCMRI

mprage_uri = Dict{String,String}()
perfusion_uri = Dict{String,String}()

function dcmhandler(channel, d, uri)
    study_uid = d[tag"Study Instance UID"]
    if DSCMRI.isdsc(d)
        println("found perfusion series")
        perfusion_uri[study_uid] = uri
        if !(study_uid in keys(mprage_uri))
            return
        end
    elseif DSCMRI.ismprage(d)
        println("found mprage series")
        mprage_uri[study_uid] = uri
        if !(study_uid in keys(perfusion_uri))
            return
        end
    else
        return
    end

    filepath_perf = perfusion_uri[study_uid]
    filepath_mprage = mprage_uri[study_uid]

    println("reading file $filepath_perf")
    nii_perf = NIfTI.niread(filepath_perf)
    println("reading file $filepath_mprage")
    nii_mprage = NIfTI.niread(filepath_mprage)
    te = d[tag"Echo Time"]
    rawvol = DSCMRI.nifti2img(nii_perf)
    mprage = DSCMRI.nifti2img(nii_mprage)
    println("starting perfusion tumor workflow")
    @time perfmaps = DSCMRI.tumor_workflow(rawvol, mprage, te)

    for (key, val) in perfmaps
        vox = DSCMRI.img2nifti(val)
        outdir = ENV["HOME"] * "/.dimseweb/nii/" * d[tag"Study Instance UID"]
        filepath = "$outdir/$(d[tag"Series Instance UID"])_$key.nii"
        NIfTI.niwrite(filepath, vox)
        dcmout = copy(d)
        dcmout[tag"Series Description"] = "$key NOT FOR CLINICAL USE"
        dcmout[tag"Image Type"][1] = "DERIVED"
        dcmout[tag"Image Type"][2] = "SECONDARY"
        dcmout[tag"Image Type"][3] = "RESAMPLED"

        publish(channel, "stored.series.nii", dcmout, uri=filepath)
    end
    
    delete!(mprage_uri, study_uid)
    delete!(perfusion_uri, study_uid)
    return true
end

consumer_loop("localhost", "tumorperfusion", ["stored.series.nii"], dcmhandler)